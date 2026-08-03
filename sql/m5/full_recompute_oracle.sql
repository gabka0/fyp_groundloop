-- Independent GroundLoop M5 PostgreSQL full-recomputation oracle.
--
-- Truth is derived only from immutable/effective base relations: typed
-- observations and currency, policy, chunks, group/requirement structure and
-- validity.  The Hall path never reads an incremental mask, histogram,
-- neighbor-count, deficiency, or matching table.  Persisted state relations
-- appear only on the mismatch side of the final audit views.

CREATE OR REPLACE VIEW groundloop_m5_oracle_snapshot AS
WITH selected_head AS (
    SELECT coalesce(
        (SELECT epoch_id FROM groundloop_m5_publication_head WHERE singleton),
        (SELECT epoch_id FROM groundloop_m4_publication_head WHERE singleton),
        (
            SELECT max(epoch_id)
            FROM groundloop_epoch
            WHERE structural_status = 'committed'
              AND semantic_status = 'sealed'
        )
    ) AS epoch_id
)
SELECT selected_head.epoch_id,
       policy.policy_version,
       policy.support_threshold,
       policy.refute_threshold
FROM selected_head
JOIN groundloop_decision_policy AS policy
  ON policy.valid_from_epoch <= selected_head.epoch_id
 AND (
     policy.valid_to_epoch IS NULL
     OR selected_head.epoch_id < policy.valid_to_epoch
 );

CREATE OR REPLACE VIEW groundloop_m5_observation_decision_oracle AS
SELECT observation.observation_id,
       observation.subject_kind,
       observation.subject_id,
       observation.chunk_version_id,
       observation.task_type,
       observation.support_score,
       observation.refute_score,
       observation.neutral_score,
       observation.eligible_for_currency,
       chunk.text_hash AS direct_text_hash,
       encode(
           digest(
               convert_to(groundloop_normalize_text_v1(chunk.text), 'UTF8'),
               'sha256'
           ),
           'hex'
       ) AS requirement_text_hash,
       snapshot.epoch_id,
       snapshot.policy_version,
       CASE
           WHEN observation.refute_score >= snapshot.refute_threshold
            AND observation.refute_score >= observation.support_score
            AND observation.refute_score >= observation.neutral_score
               THEN 'refute'
           WHEN observation.support_score >= snapshot.support_threshold
            AND observation.support_score > observation.refute_score
            AND observation.support_score > observation.neutral_score
               THEN 'support'
           ELSE 'neutral'
       END AS label
FROM groundloop_observation_currency AS currency
JOIN groundloop_semantic_observation AS observation
  ON observation.observation_id = currency.observation_id
 AND observation.subject_kind = currency.subject_kind
 AND observation.subject_id = currency.subject_id
 AND observation.chunk_version_id = currency.chunk_version_id
 AND observation.task_type = currency.task_type
JOIN groundloop_chunk_version AS chunk
  ON chunk.chunk_version_id = observation.chunk_version_id
CROSS JOIN groundloop_m5_oracle_snapshot AS snapshot
WHERE chunk.valid_from_epoch <= snapshot.epoch_id
  AND (chunk.valid_to_epoch IS NULL OR snapshot.epoch_id < chunk.valid_to_epoch);

CREATE OR REPLACE VIEW groundloop_m5_active_requirement_edge_oracle AS
SELECT requirement.requirement_version_id,
       requirement.group_version_id,
       requirement.ordinal AS requirement_ordinal,
       decision.requirement_text_hash AS text_hash,
       array_agg(
           decision.observation_id ORDER BY decision.observation_id COLLATE "C"
       ) AS active_observation_ids
FROM groundloop_m5_requirement_version AS requirement
JOIN groundloop_m5_group_validity AS validity
  ON validity.group_version_id = requirement.group_version_id
JOIN groundloop_m5_oracle_snapshot AS snapshot
  ON validity.valid_from_epoch <= snapshot.epoch_id
 AND (
     validity.valid_to_epoch IS NULL
     OR snapshot.epoch_id < validity.valid_to_epoch
 )
JOIN groundloop_m5_observation_decision_oracle AS decision
  ON decision.subject_kind = 'requirement'
 AND decision.subject_id = requirement.requirement_version_id
 AND decision.task_type = 'verify_requirement_v1'
 AND decision.eligible_for_currency
 AND decision.label = 'support'
WHERE requirement.lifecycle_state = 'PUBLISHED'
GROUP BY requirement.requirement_version_id,
         requirement.group_version_id,
         requirement.ordinal,
         decision.requirement_text_hash;

CREATE OR REPLACE VIEW groundloop_m5_requirement_state_oracle AS
WITH active_requirement AS (
    SELECT requirement.requirement_version_id
    FROM groundloop_m5_requirement_version AS requirement
    JOIN groundloop_m5_group_validity AS validity
      ON validity.group_version_id = requirement.group_version_id
    JOIN groundloop_m5_oracle_snapshot AS snapshot
      ON validity.valid_from_epoch <= snapshot.epoch_id
     AND (
         validity.valid_to_epoch IS NULL
         OR snapshot.epoch_id < validity.valid_to_epoch
     )
    WHERE requirement.lifecycle_state = 'PUBLISHED'
)
SELECT active.requirement_version_id,
       coalesce(
           array_agg(
               DISTINCT edge.text_hash COLLATE "C"
               ORDER BY edge.text_hash COLLATE "C"
           )
               FILTER (WHERE edge.text_hash IS NOT NULL),
           ARRAY[]::text[]
       ) AS witness_hashes,
       coalesce(
           array_agg(
               DISTINCT observation_id COLLATE "C"
               ORDER BY observation_id COLLATE "C"
           )
               FILTER (WHERE observation_id IS NOT NULL),
           ARRAY[]::text[]
       ) AS supporting_observation_ids,
       count(DISTINCT edge.text_hash)::integer AS witness_count,
       count(DISTINCT edge.text_hash) > 0 AS satisfied
FROM active_requirement AS active
LEFT JOIN groundloop_m5_active_requirement_edge_oracle AS edge
  ON edge.requirement_version_id = active.requirement_version_id
LEFT JOIN LATERAL unnest(edge.active_observation_ids) AS observation_id
  ON true
GROUP BY active.requirement_version_id;

CREATE OR REPLACE VIEW groundloop_m5_group_subset_hall_oracle AS
WITH active_group AS (
    SELECT validity.group_version_id,
           count(requirement.requirement_version_id)::integer AS requirement_count
    FROM groundloop_m5_group_validity AS validity
    JOIN groundloop_m5_oracle_snapshot AS snapshot
      ON validity.valid_from_epoch <= snapshot.epoch_id
     AND (
         validity.valid_to_epoch IS NULL
         OR snapshot.epoch_id < validity.valid_to_epoch
     )
    JOIN groundloop_m5_requirement_version AS requirement
      ON requirement.group_version_id = validity.group_version_id
     AND requirement.lifecycle_state = 'PUBLISHED'
    GROUP BY validity.group_version_id
)
SELECT active_group.group_version_id,
       active_group.requirement_count,
       subset.subset_mask,
       bit_count(subset.subset_mask::bit(8))::integer AS subset_size,
       count(DISTINCT edge.text_hash)::integer AS neighbor_count,
       bit_count(subset.subset_mask::bit(8))::integer
           - count(DISTINCT edge.text_hash)::integer AS deficiency
FROM active_group
CROSS JOIN LATERAL generate_series(
    1,
    (1 << active_group.requirement_count) - 1
) AS subset(subset_mask)
LEFT JOIN groundloop_m5_active_requirement_edge_oracle AS edge
  ON edge.group_version_id = active_group.group_version_id
 AND (subset.subset_mask & (1 << edge.requirement_ordinal)) <> 0
GROUP BY active_group.group_version_id,
         active_group.requirement_count,
         subset.subset_mask;

CREATE OR REPLACE VIEW groundloop_m5_group_state_oracle AS
WITH aggregated AS (
    SELECT hall.group_version_id,
           hall.requirement_count,
           count(DISTINCT requirement.requirement_version_id)
               FILTER (WHERE requirement_state.satisfied)::integer
               AS satisfied_count,
           greatest(0, max(hall.deficiency))::integer AS maximum_deficiency
    FROM groundloop_m5_group_subset_hall_oracle AS hall
    JOIN groundloop_m5_requirement_version AS requirement
      ON requirement.group_version_id = hall.group_version_id
     AND requirement.lifecycle_state = 'PUBLISHED'
    JOIN groundloop_m5_requirement_state_oracle AS requirement_state
      ON requirement_state.requirement_version_id =
         requirement.requirement_version_id
    GROUP BY hall.group_version_id, hall.requirement_count
)
SELECT group_version_id,
       requirement_count,
       satisfied_count,
       requirement_count - maximum_deficiency AS matching_size,
       maximum_deficiency = 0 AS complete
FROM aggregated;

CREATE OR REPLACE FUNCTION groundloop_m5_binomial(n_value integer, k_value integer)
RETURNS bigint
LANGUAGE plpgsql
IMMUTABLE
STRICT
PARALLEL SAFE
AS $$
DECLARE
    result bigint := 1;
    step integer;
    reduced_k integer;
BEGIN
    IF n_value < 0 OR k_value < 0 OR k_value > n_value THEN
        RETURN 0;
    END IF;
    reduced_k := least(k_value, n_value - k_value);
    IF reduced_k = 0 THEN
        RETURN 1;
    END IF;
    FOR step IN 1..reduced_k LOOP
        result := (result * (n_value - reduced_k + step)) / step;
    END LOOP;
    RETURN result;
END;
$$;

CREATE OR REPLACE FUNCTION groundloop_m5_assignment_preflight_bound(
    requirement_count integer,
    hash_count integer
)
RETURNS bigint
LANGUAGE sql
IMMUTABLE
STRICT
PARALLEL SAFE
AS $$
    SELECT coalesce(
        sum(
            groundloop_m5_binomial(hash_count, chosen_count)
            * (requirement_count - chosen_count + 1)
        ),
        0
    )::bigint
    FROM generate_series(
        0,
        least(requirement_count, hash_count)
    ) AS chosen_count;
$$;

CREATE OR REPLACE VIEW groundloop_m5_assignment_audit_preflight AS
WITH metrics AS (
    SELECT group_state.group_version_id,
           group_state.requirement_count,
           count(DISTINCT edge.text_hash)::integer AS hash_count,
           count(edge.text_hash)::integer AS edge_count
    FROM groundloop_m5_group_state_oracle AS group_state
    LEFT JOIN groundloop_m5_active_requirement_edge_oracle AS edge
      ON edge.group_version_id = group_state.group_version_id
    GROUP BY group_state.group_version_id, group_state.requirement_count
)
SELECT metrics.group_version_id,
       metrics.requirement_count,
       metrics.hash_count,
       metrics.edge_count,
       groundloop_m5_assignment_preflight_bound(
           metrics.requirement_count,
           metrics.hash_count
       ) AS preflight_state_bound,
       CASE
           WHEN metrics.hash_count > 16
             OR metrics.edge_count > 128
             OR groundloop_m5_assignment_preflight_bound(
                    metrics.requirement_count,
                    metrics.hash_count
                ) > 100000
               THEN 'ASSIGNMENT_AUDIT_CAP_EXCEEDED'
           ELSE 'ASSIGNMENT_AUDIT_ELIGIBLE'
       END AS preflight_status
FROM metrics;

CREATE OR REPLACE VIEW groundloop_m5_assignment_hash_ordinal AS
SELECT edge.group_version_id,
       edge.text_hash,
       dense_rank() OVER (
           PARTITION BY edge.group_version_id
           ORDER BY edge.text_hash
       )::integer - 1 AS hash_ordinal
FROM (
    SELECT DISTINCT group_version_id, text_hash
    FROM groundloop_m5_active_requirement_edge_oracle
) AS edge;

CREATE OR REPLACE VIEW groundloop_m5_assignment_audit_oracle AS
WITH RECURSIVE eligible AS (
    SELECT *
    FROM groundloop_m5_assignment_audit_preflight
    WHERE preflight_status = 'ASSIGNMENT_AUDIT_ELIGIBLE'
),
assignment_state (
    group_version_id, next_requirement_ordinal, used_hash_mask
) AS (
    SELECT eligible.group_version_id,
           0 AS next_requirement_ordinal,
           0 AS used_hash_mask
    FROM eligible
    UNION
    SELECT state.group_version_id,
           state.next_requirement_ordinal + 1,
           transition.used_hash_mask
    FROM assignment_state AS state
    JOIN eligible
      ON eligible.group_version_id = state.group_version_id
    CROSS JOIN LATERAL (
        SELECT state.used_hash_mask
        UNION
        SELECT state.used_hash_mask | (1 << hash_ordinal.hash_ordinal)
        FROM groundloop_m5_active_requirement_edge_oracle AS edge
        JOIN groundloop_m5_assignment_hash_ordinal AS hash_ordinal
          ON hash_ordinal.group_version_id = edge.group_version_id
         AND hash_ordinal.text_hash = edge.text_hash
        WHERE edge.group_version_id = state.group_version_id
          AND edge.requirement_ordinal = state.next_requirement_ordinal
          AND (
              state.used_hash_mask & (1 << hash_ordinal.hash_ordinal)
          ) = 0
    ) AS transition
    WHERE state.next_requirement_ordinal < eligible.requirement_count
),
state_counts AS (
    SELECT group_version_id, count(*)::bigint AS visited_state_count
    FROM assignment_state
    GROUP BY group_version_id
),
final_assignment AS (
    SELECT state.group_version_id,
           max(bit_count(state.used_hash_mask::bit(16)))::integer
               AS assignment_matching_size
    FROM assignment_state AS state
    JOIN eligible
      ON eligible.group_version_id = state.group_version_id
     AND state.next_requirement_ordinal = eligible.requirement_count
    GROUP BY state.group_version_id
)
SELECT preflight.group_version_id,
       preflight.requirement_count,
       preflight.hash_count,
       preflight.edge_count,
       preflight.preflight_state_bound,
       state_counts.visited_state_count,
       final_assignment.assignment_matching_size,
       hall.matching_size AS hall_matching_size,
       CASE
           WHEN preflight.preflight_status = 'ASSIGNMENT_AUDIT_CAP_EXCEEDED'
               THEN 'ASSIGNMENT_AUDIT_CAP_EXCEEDED'
           WHEN final_assignment.assignment_matching_size = hall.matching_size
               THEN 'ASSIGNMENT_AUDIT_OK'
           ELSE 'ASSIGNMENT_AUDIT_MISMATCH'
       END AS audit_status
FROM groundloop_m5_assignment_audit_preflight AS preflight
JOIN groundloop_m5_group_state_oracle AS hall
  ON hall.group_version_id = preflight.group_version_id
LEFT JOIN state_counts
  ON state_counts.group_version_id = preflight.group_version_id
LEFT JOIN final_assignment
  ON final_assignment.group_version_id = preflight.group_version_id;

CREATE OR REPLACE VIEW groundloop_m5_direct_claim_state_oracle AS
WITH contributing AS (
    SELECT *
    FROM groundloop_m5_observation_decision_oracle
    WHERE subject_kind = 'claim'
      AND eligible_for_currency
      AND label <> 'neutral'
),
aggregated AS (
    SELECT claim.claim_id,
           count(DISTINCT contributing.direct_text_hash)
               FILTER (WHERE contributing.label = 'support')::integer
               AS support_count,
           count(DISTINCT contributing.direct_text_hash)
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
                   contributing.observation_id
                   ORDER BY contributing.observation_id COLLATE "C"
               ) FILTER (WHERE contributing.label = 'support'),
               ARRAY[]::text[]
           ) AS supporting_observation_ids,
           coalesce(
               array_agg(
                   contributing.observation_id
                   ORDER BY contributing.observation_id COLLATE "C"
               ) FILTER (WHERE contributing.label = 'refute'),
               ARRAY[]::text[]
           ) AS refuting_observation_ids
    FROM groundloop_claim AS claim
    LEFT JOIN contributing
      ON contributing.subject_id = claim.claim_id
    GROUP BY claim.claim_id
)
SELECT * FROM aggregated;

CREATE OR REPLACE VIEW groundloop_m5_claim_state_oracle AS
WITH complete_group AS (
    SELECT family.claim_id,
           group_state.group_version_id
    FROM groundloop_m5_group_state_oracle AS group_state
    JOIN groundloop_m5_group_version AS group_row
      ON group_row.group_version_id = group_state.group_version_id
    JOIN groundloop_m5_group_family AS family
      ON family.group_family_id = group_row.group_family_id
    WHERE group_state.complete
),
composed AS (
    SELECT direct.claim_id,
           direct.support_count,
           direct.refute_count,
           direct.best_support_score,
           direct.best_refute_score,
           direct.supporting_observation_ids,
           direct.refuting_observation_ids,
           count(complete_group.group_version_id)::integer AS complete_group_count,
           coalesce(
               array_agg(
                   complete_group.group_version_id
                   ORDER BY complete_group.group_version_id COLLATE "C"
               ) FILTER (WHERE complete_group.group_version_id IS NOT NULL),
               ARRAY[]::text[]
           ) AS complete_group_ids
    FROM groundloop_m5_direct_claim_state_oracle AS direct
    LEFT JOIN complete_group
      ON complete_group.claim_id = direct.claim_id
    GROUP BY direct.claim_id,
             direct.support_count,
             direct.refute_count,
             direct.best_support_score,
             direct.best_refute_score,
             direct.supporting_observation_ids,
             direct.refuting_observation_ids
)
SELECT composed.*,
       CASE
           WHEN (support_count > 0 OR complete_group_count > 0)
            AND refute_count > 0 THEN 'conflicted'
           WHEN support_count > 0 OR complete_group_count > 0 THEN 'supported'
           WHEN refute_count > 0 THEN 'refuted'
           ELSE 'unsupported'
       END AS status
FROM composed;

CREATE OR REPLACE VIEW groundloop_m5_answer_state_oracle AS
WITH aggregated AS (
    SELECT answer.answer_version_id,
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
    JOIN groundloop_m5_claim_state_oracle AS state
      ON state.claim_id = claim.claim_id
    GROUP BY answer.answer_version_id
)
SELECT aggregated.*,
       CASE
           WHEN refuted_count > 0 THEN 'contradicted'
           WHEN conflicted_count > 0 THEN 'conflicted'
           WHEN required_claim_count > 0
            AND supported_count = required_claim_count THEN 'valid'
           WHEN supported_count > 0 THEN 'partially_supported'
           ELSE 'unsupported'
       END AS status
FROM aggregated;

CREATE OR REPLACE VIEW groundloop_m5_group_certificate_validity_oracle AS
SELECT artifact.certificate_digest,
       artifact.group_version_id,
       artifact.decision_policy_version,
       (
           artifact.certificate_digest =
               groundloop_m5_expected_group_certificate(artifact.certificate_digest)
           AND artifact.decision_policy_version = snapshot.policy_version
           AND group_state.complete
           AND artifact.requirement_count = group_state.requirement_count
           AND (
               SELECT count(*)
               FROM groundloop_m5_group_certificate_artifact_row AS counted_row
               WHERE counted_row.certificate_digest = artifact.certificate_digest
           ) = artifact.requirement_count
           AND NOT EXISTS (
               SELECT 1
               FROM groundloop_m5_group_certificate_artifact_row AS artifact_row
               JOIN groundloop_m5_requirement_version AS requirement
                 ON requirement.requirement_version_id =
                    artifact_row.requirement_version_id
               WHERE artifact_row.certificate_digest = artifact.certificate_digest
                 AND (
                     requirement.group_version_id <> artifact.group_version_id
                     OR requirement.ordinal <> artifact_row.requirement_ordinal
                     OR NOT EXISTS (
                         SELECT 1
                         FROM groundloop_m5_observation_decision_oracle AS decision
                         WHERE decision.observation_id =
                               artifact_row.selected_observation_id
                           AND decision.subject_kind = 'requirement'
                           AND decision.subject_id =
                               artifact_row.requirement_version_id
                           AND decision.task_type = 'verify_requirement_v1'
                           AND decision.eligible_for_currency
                           AND decision.label = 'support'
                           AND decision.requirement_text_hash = artifact_row.text_hash
                     )
                 )
           )
       ) AS certificate_valid
FROM groundloop_m5_group_certificate_artifact AS artifact
JOIN groundloop_m5_group_state_oracle AS group_state
  ON group_state.group_version_id = artifact.group_version_id
CROSS JOIN groundloop_m5_oracle_snapshot AS snapshot;

CREATE OR REPLACE VIEW groundloop_m5_claim_certificate_validity_oracle AS
SELECT artifact.certificate_digest,
       artifact.claim_id,
       artifact.decision_policy_version,
       (
           artifact.certificate_digest =
               groundloop_m5_expected_claim_certificate(artifact.certificate_digest)
           AND artifact.decision_policy_version = snapshot.policy_version
           AND (
               (artifact.support_kind = 'none'
                AND state.support_count = 0
                AND state.complete_group_count = 0)
               OR
               (artifact.support_kind = 'direct'
                AND artifact.direct_support_observation_id =
                    state.supporting_observation_ids[1])
               OR
               (artifact.support_kind = 'group'
                AND state.support_count = 0
                AND artifact.group_version_id = state.complete_group_ids[1]
                AND EXISTS (
                    SELECT 1
                    FROM groundloop_m5_group_certificate_validity_oracle AS group_validity
                    WHERE group_validity.certificate_digest =
                          artifact.group_certificate_digest
                      AND group_validity.group_version_id = artifact.group_version_id
                      AND group_validity.certificate_valid
                ))
           )
           AND artifact.direct_refute_observation_id IS NOT DISTINCT FROM
               state.refuting_observation_ids[1]
       ) AS certificate_valid
FROM groundloop_m5_claim_certificate_artifact AS artifact
JOIN groundloop_m5_claim_state_oracle AS state
  ON state.claim_id = artifact.claim_id
CROSS JOIN groundloop_m5_oracle_snapshot AS snapshot;

CREATE OR REPLACE FUNCTION groundloop_m5_group_certificate_valid_at(
    certificate_digest_to_check char(64),
    epoch_id_to_read bigint,
    revision_to_read bigint
)
RETURNS boolean
LANGUAGE sql
STABLE
STRICT
AS $$
    SELECT EXISTS (
        SELECT 1
        FROM groundloop_m5_group_certificate_artifact AS artifact
        JOIN groundloop_m5_effective_group_version AS effective_group
          ON effective_group.epoch_id = epoch_id_to_read
         AND effective_group.group_version_id = artifact.group_version_id
        JOIN groundloop_m5_update AS update_row
          ON update_row.epoch_id = epoch_id_to_read
        WHERE artifact.certificate_digest = certificate_digest_to_check
          AND artifact.decision_policy_version =
              update_row.decision_policy_version
          AND artifact.certificate_digest =
              groundloop_m5_expected_group_certificate(artifact.certificate_digest)
          AND artifact.requirement_count = (
              SELECT count(*)
              FROM groundloop_m5_effective_requirement_version AS requirement
              WHERE requirement.epoch_id = epoch_id_to_read
                AND requirement.group_version_id = artifact.group_version_id
          )
          AND NOT EXISTS (
              SELECT 1
              FROM groundloop_m5_group_certificate_artifact_row AS artifact_row
              WHERE artifact_row.certificate_digest = artifact.certificate_digest
                AND NOT EXISTS (
                    SELECT 1
                    FROM groundloop_m5_effective_requirement_version AS requirement
                    JOIN groundloop_semantic_observation AS observation
                      ON observation.observation_id =
                         artifact_row.selected_observation_id
                    JOIN groundloop_m5_currency_at(
                        epoch_id_to_read, revision_to_read
                    ) AS currency
                      ON currency.observation_id = observation.observation_id
                     AND currency.subject_kind = observation.subject_kind
                     AND currency.subject_id = observation.subject_id
                     AND currency.chunk_version_id = observation.chunk_version_id
                     AND currency.task_type = observation.task_type
                    JOIN groundloop_chunk_version AS chunk
                      ON chunk.chunk_version_id = observation.chunk_version_id
                    JOIN groundloop_decision_policy AS policy
                      ON policy.policy_version = update_row.decision_policy_version
                    WHERE requirement.epoch_id = epoch_id_to_read
                      AND requirement.requirement_version_id =
                          artifact_row.requirement_version_id
                      AND requirement.group_version_id = artifact.group_version_id
                      AND requirement.ordinal = artifact_row.requirement_ordinal
                      AND observation.subject_kind = 'requirement'
                      AND observation.subject_id =
                          artifact_row.requirement_version_id
                      AND observation.task_type = 'verify_requirement_v1'
                      AND observation.eligible_for_currency
                      AND observation.support_score >= policy.support_threshold
                      AND observation.support_score > observation.refute_score
                      AND observation.support_score > observation.neutral_score
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
                      AND chunk.valid_from_epoch <= epoch_id_to_read
                      AND (
                          chunk.valid_to_epoch IS NULL
                          OR epoch_id_to_read < chunk.valid_to_epoch
                      )
                )
          )
          AND (
              SELECT count(*)
              FROM groundloop_m5_group_certificate_artifact_row AS counted_row
              WHERE counted_row.certificate_digest = artifact.certificate_digest
          ) = artifact.requirement_count
    );
$$;

CREATE OR REPLACE FUNCTION groundloop_m5_group_certificate_bindings_at(
    epoch_id_to_read bigint,
    revision_to_read bigint
)
RETURNS TABLE (group_version_id text, certificate_digest char(64))
LANGUAGE sql
STABLE
STRICT
AS $$
    SELECT effective.group_version_id, working.certificate_digest
    FROM groundloop_m5_effective_group_version AS effective
    JOIN groundloop_m5_working_group_certificate_binding AS working
      ON working.epoch_id = effective.epoch_id
     AND working.group_version_id = effective.group_version_id
     AND working.valid_from_revision <= revision_to_read
     AND (
         working.valid_to_revision IS NULL
         OR revision_to_read < working.valid_to_revision
     )
    WHERE effective.epoch_id = epoch_id_to_read
    UNION ALL
    SELECT effective.group_version_id, published.certificate_digest
    FROM groundloop_m5_effective_group_version AS effective
    JOIN groundloop_m5_update AS update_row
      ON update_row.epoch_id = effective.epoch_id
    JOIN groundloop_m5_published_group_certificate_binding AS published
      ON published.group_version_id = effective.group_version_id
     AND published.valid_from_epoch <= update_row.previous_published_epoch_id
     AND (
         published.valid_to_epoch IS NULL
         OR update_row.previous_published_epoch_id < published.valid_to_epoch
     )
    WHERE effective.epoch_id = epoch_id_to_read
      AND NOT EXISTS (
          SELECT 1
          FROM groundloop_m5_working_group_certificate_binding AS history
          WHERE history.epoch_id = effective.epoch_id
            AND history.group_version_id = effective.group_version_id
      );
$$;

CREATE OR REPLACE FUNCTION groundloop_m5_claim_certificate_valid_at(
    certificate_digest_to_check char(64),
    epoch_id_to_read bigint,
    revision_to_read bigint
)
RETURNS boolean
LANGUAGE plpgsql
STABLE
STRICT
AS $$
DECLARE
    artifact record;
    policy_version_to_read text;
    expected_support_kind text;
    expected_direct_support text;
    expected_group_version text;
    expected_group_certificate char(64);
    expected_direct_refute text;
BEGIN
    SELECT *
    INTO artifact
    FROM groundloop_m5_claim_certificate_artifact
    WHERE certificate_digest = certificate_digest_to_check;
    IF NOT FOUND THEN
        RETURN false;
    END IF;

    SELECT decision_policy_version
    INTO policy_version_to_read
    FROM groundloop_m5_update
    WHERE epoch_id = epoch_id_to_read;
    IF NOT FOUND
       OR artifact.decision_policy_version <> policy_version_to_read
       OR artifact.certificate_digest <>
          groundloop_m5_expected_claim_certificate(artifact.certificate_digest) THEN
        RETURN false;
    END IF;

    SELECT min(observation.observation_id COLLATE "C")
    INTO expected_direct_support
    FROM groundloop_m5_currency_at(
        epoch_id_to_read, revision_to_read
    ) AS currency
    JOIN groundloop_semantic_observation AS observation
      ON observation.observation_id = currency.observation_id
     AND observation.subject_kind = currency.subject_kind
     AND observation.subject_id = currency.subject_id
     AND observation.chunk_version_id = currency.chunk_version_id
     AND observation.task_type = currency.task_type
    JOIN groundloop_chunk_version AS chunk
      ON chunk.chunk_version_id = observation.chunk_version_id
    JOIN groundloop_decision_policy AS policy
      ON policy.policy_version = policy_version_to_read
    WHERE observation.subject_kind = 'claim'
      AND observation.subject_id = artifact.claim_id
      AND observation.eligible_for_currency
      AND observation.support_score >= policy.support_threshold
      AND observation.support_score > observation.refute_score
      AND observation.support_score > observation.neutral_score
      AND chunk.valid_from_epoch <= epoch_id_to_read
      AND (
          chunk.valid_to_epoch IS NULL
          OR epoch_id_to_read < chunk.valid_to_epoch
      );

    SELECT min(observation.observation_id COLLATE "C")
    INTO expected_direct_refute
    FROM groundloop_m5_currency_at(
        epoch_id_to_read, revision_to_read
    ) AS currency
    JOIN groundloop_semantic_observation AS observation
      ON observation.observation_id = currency.observation_id
     AND observation.subject_kind = currency.subject_kind
     AND observation.subject_id = currency.subject_id
     AND observation.chunk_version_id = currency.chunk_version_id
     AND observation.task_type = currency.task_type
    JOIN groundloop_chunk_version AS chunk
      ON chunk.chunk_version_id = observation.chunk_version_id
    JOIN groundloop_decision_policy AS policy
      ON policy.policy_version = policy_version_to_read
    WHERE observation.subject_kind = 'claim'
      AND observation.subject_id = artifact.claim_id
      AND observation.eligible_for_currency
      AND observation.refute_score >= policy.refute_threshold
      AND observation.refute_score >= observation.support_score
      AND observation.refute_score >= observation.neutral_score
      AND chunk.valid_from_epoch <= epoch_id_to_read
      AND (
          chunk.valid_to_epoch IS NULL
          OR epoch_id_to_read < chunk.valid_to_epoch
      );

    SELECT state.group_version_id, state.certificate_digest
    INTO expected_group_version, expected_group_certificate
    FROM groundloop_m5_group_certificate_bindings_at(
        epoch_id_to_read, revision_to_read
    ) AS state
    JOIN groundloop_m5_effective_group_version AS effective
      ON effective.epoch_id = epoch_id_to_read
     AND effective.group_version_id = state.group_version_id
    WHERE effective.claim_id = artifact.claim_id
      AND groundloop_m5_group_certificate_valid_at(
          state.certificate_digest,
          epoch_id_to_read,
          revision_to_read
      )
    ORDER BY state.group_version_id COLLATE "C"
    LIMIT 1;

    IF expected_direct_support IS NOT NULL THEN
        expected_support_kind := 'direct';
        expected_group_version := NULL;
        expected_group_certificate := NULL;
    ELSIF expected_group_version IS NOT NULL THEN
        expected_support_kind := 'group';
    ELSE
        expected_support_kind := 'none';
        expected_group_certificate := NULL;
    END IF;

    RETURN artifact.support_kind = expected_support_kind
       AND artifact.direct_support_observation_id IS NOT DISTINCT FROM
           expected_direct_support
       AND artifact.group_version_id IS NOT DISTINCT FROM expected_group_version
       AND artifact.group_certificate_digest IS NOT DISTINCT FROM
           expected_group_certificate
       AND artifact.direct_refute_observation_id IS NOT DISTINCT FROM
           expected_direct_refute;
END;
$$;

CREATE OR REPLACE VIEW groundloop_m5_requirement_state_mismatches AS
SELECT coalesce(materialized.requirement_version_id, oracle.requirement_version_id)
           AS requirement_version_id,
       materialized AS materialized_row,
       oracle AS oracle_row
FROM groundloop_m5_requirement_state_materialized AS materialized
FULL OUTER JOIN groundloop_m5_requirement_state_oracle AS oracle
  ON oracle.requirement_version_id = materialized.requirement_version_id
WHERE (
    materialized.witness_hashes,
    materialized.supporting_observation_ids,
    materialized.witness_count,
    materialized.satisfied
) IS DISTINCT FROM (
    oracle.witness_hashes,
    oracle.supporting_observation_ids,
    oracle.witness_count,
    oracle.satisfied
);

CREATE OR REPLACE VIEW groundloop_m5_group_state_mismatches AS
SELECT coalesce(materialized.group_version_id, oracle.group_version_id)
           AS group_version_id,
       materialized AS materialized_row,
       oracle AS oracle_row
FROM groundloop_m5_group_state_materialized AS materialized
FULL OUTER JOIN groundloop_m5_group_state_oracle AS oracle
  ON oracle.group_version_id = materialized.group_version_id
WHERE (
    materialized.requirement_count,
    materialized.satisfied_count,
    materialized.matching_size,
    materialized.complete
) IS DISTINCT FROM (
    oracle.requirement_count,
    oracle.satisfied_count,
    oracle.matching_size,
    oracle.complete
);

CREATE OR REPLACE VIEW groundloop_m5_claim_state_mismatches AS
SELECT coalesce(materialized.claim_id, oracle.claim_id) AS claim_id,
       materialized AS materialized_row,
       oracle AS oracle_row
FROM groundloop_m5_claim_state_materialized AS materialized
FULL OUTER JOIN groundloop_m5_claim_state_oracle AS oracle
  ON oracle.claim_id = materialized.claim_id
WHERE (
    materialized.support_count,
    materialized.refute_count,
    materialized.best_support_score,
    materialized.best_refute_score,
    materialized.supporting_observation_ids,
    materialized.refuting_observation_ids,
    materialized.complete_group_count,
    materialized.complete_group_ids,
    materialized.status
) IS DISTINCT FROM (
    oracle.support_count,
    oracle.refute_count,
    oracle.best_support_score,
    oracle.best_refute_score,
    oracle.supporting_observation_ids,
    oracle.refuting_observation_ids,
    oracle.complete_group_count,
    oracle.complete_group_ids,
    oracle.status
);

CREATE OR REPLACE VIEW groundloop_m5_answer_state_mismatches AS
SELECT coalesce(materialized.answer_version_id, oracle.answer_version_id)
           AS answer_version_id,
       materialized AS materialized_row,
       oracle AS oracle_row
FROM groundloop_m5_answer_state_materialized AS materialized
FULL OUTER JOIN groundloop_m5_answer_state_oracle AS oracle
  ON oracle.answer_version_id = materialized.answer_version_id
WHERE (
    materialized.required_claim_count,
    materialized.supported_count,
    materialized.unsupported_count,
    materialized.refuted_count,
    materialized.conflicted_count,
    materialized.status
) IS DISTINCT FROM (
    oracle.required_claim_count,
    oracle.supported_count,
    oracle.unsupported_count,
    oracle.refuted_count,
    oracle.conflicted_count,
    oracle.status
);

CREATE OR REPLACE VIEW groundloop_m5_certificate_mismatches AS
SELECT 'group'::text AS certificate_kind,
       state.certificate_digest::text AS certificate_digest
FROM groundloop_m5_group_state_materialized AS state
LEFT JOIN groundloop_m5_group_certificate_validity_oracle AS validity
  ON validity.certificate_digest = state.certificate_digest
WHERE state.complete
  AND validity.certificate_valid IS DISTINCT FROM true
UNION ALL
SELECT 'claim'::text AS certificate_kind,
       state.certificate_digest::text AS certificate_digest
FROM groundloop_m5_claim_state_materialized AS state
LEFT JOIN groundloop_m5_claim_certificate_validity_oracle AS validity
  ON validity.certificate_digest = state.certificate_digest
WHERE validity.certificate_valid IS DISTINCT FROM true;

CREATE OR REPLACE VIEW groundloop_m5_assignment_audit_mismatches AS
SELECT *
FROM groundloop_m5_assignment_audit_oracle
WHERE audit_status = 'ASSIGNMENT_AUDIT_MISMATCH';

CREATE OR REPLACE VIEW groundloop_m5_oracle_mismatch_counts AS
SELECT (
           SELECT count(*)
           FROM groundloop_m5_requirement_state_mismatches
       )::bigint AS requirement_mismatches,
       (
           SELECT count(*)
           FROM groundloop_m5_group_state_mismatches
       )::bigint AS group_mismatches,
       (
           SELECT count(*)
           FROM groundloop_m5_claim_state_mismatches
       )::bigint AS claim_mismatches,
       (
           SELECT count(*)
           FROM groundloop_m5_answer_state_mismatches
       )::bigint AS answer_mismatches,
       (
           SELECT count(*)
           FROM groundloop_m5_certificate_mismatches
       )::bigint AS certificate_mismatches,
       (
           SELECT count(*)
           FROM groundloop_m5_assignment_audit_mismatches
       )::bigint AS assignment_mismatches,
       (
           SELECT count(*)
           FROM groundloop_m5_assignment_audit_oracle
           WHERE audit_status = 'ASSIGNMENT_AUDIT_CAP_EXCEEDED'
       )::bigint AS assignment_cap_exceeded;
