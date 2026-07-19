-- Failure-safe M4 structural overlays.
--
-- Pending/failed M4 versions remain immutable history, but they are not
-- globally current.  Working activity is resolved relative to an event's
-- previous published epoch plus only that event's inserted/deactivated rows.

DROP INDEX groundloop_one_active_document_version;

CREATE INDEX groundloop_open_document_versions
    ON groundloop_document_version(document_id, valid_from_epoch)
    WHERE valid_to_epoch IS NULL;

DROP INDEX groundloop_one_current_frontier_entry;
DROP INDEX groundloop_current_frontier_by_chunk;

CREATE INDEX groundloop_frontier_by_pair_and_epoch
    ON groundloop_candidate_frontier(
        claim_id, chunk_version_id, candidate_policy_id, valid_from_epoch
    );

CREATE INDEX groundloop_frontier_by_chunk_and_epoch
    ON groundloop_candidate_frontier(
        chunk_version_id, candidate_policy_id, valid_from_epoch
    );

CREATE TABLE groundloop_m4_structural_deactivation (
    epoch_id bigint NOT NULL REFERENCES groundloop_m4_update(epoch_id),
    document_version_id text NOT NULL
        REFERENCES groundloop_document_version(document_version_id),
    PRIMARY KEY (epoch_id, document_version_id)
);

-- The corpus-level document row is required by the canonical version FK, but
-- source/authority metadata from an unsealed event must not become published.
-- New document identities therefore use a neutral canonical placeholder and
-- keep their event payload here until the seal transaction promotes it.
CREATE TABLE groundloop_m4_document_metadata_overlay (
    epoch_id bigint NOT NULL REFERENCES groundloop_m4_update(epoch_id),
    document_id text NOT NULL REFERENCES groundloop_document(document_id),
    source_uri text,
    authority_class text NOT NULL CHECK (btrim(authority_class) <> ''),
    PRIMARY KEY (epoch_id, document_id)
);

CREATE TRIGGER groundloop_m4_document_metadata_overlay_immutable
BEFORE UPDATE OR DELETE ON groundloop_m4_document_metadata_overlay
FOR EACH ROW EXECUTE FUNCTION groundloop_reject_immutable_m4_update();

-- Registry membership is independent of whether an embedding index has been
-- built.  It freezes the relational object universe used by historical M4
-- grounding and Surface-C recomputation.
CREATE TABLE groundloop_m4_claim_registry_member (
    claim_registry_snapshot_id text NOT NULL,
    claim_id text NOT NULL REFERENCES groundloop_claim(claim_id),
    member_ordinal integer NOT NULL CHECK (member_ordinal >= 0),
    PRIMARY KEY (claim_registry_snapshot_id, claim_id),
    UNIQUE (claim_registry_snapshot_id, member_ordinal)
);

CREATE TRIGGER groundloop_m4_claim_registry_member_immutable
BEFORE UPDATE OR DELETE ON groundloop_m4_claim_registry_member
FOR EACH ROW EXECUTE FUNCTION groundloop_reject_immutable_m4_update();

CREATE FUNCTION groundloop_reject_immutable_structural_deactivation()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION 'immutable M4 structural deactivation cannot change';
END;
$$;

CREATE TRIGGER groundloop_m4_structural_deactivation_immutable
BEFORE UPDATE OR DELETE ON groundloop_m4_structural_deactivation
FOR EACH ROW EXECUTE FUNCTION
    groundloop_reject_immutable_structural_deactivation();

CREATE VIEW groundloop_m4_effective_document_version AS
SELECT update_row.epoch_id,
       version.document_version_id,
       version.document_id,
       version.content_hash,
       version.valid_from_epoch,
       version.valid_to_epoch
FROM groundloop_m4_update AS update_row
JOIN groundloop_document_version AS version
  ON update_row.previous_published_epoch_id IS NOT NULL
 AND version.valid_from_epoch <= update_row.previous_published_epoch_id
 AND (
     version.valid_to_epoch IS NULL
     OR update_row.previous_published_epoch_id < version.valid_to_epoch
 )
JOIN groundloop_epoch AS creator
  ON creator.epoch_id = version.valid_from_epoch
 AND creator.semantic_status = 'sealed'
WHERE NOT EXISTS (
    SELECT 1
    FROM groundloop_m4_structural_deactivation AS deactivation
    WHERE deactivation.epoch_id = update_row.epoch_id
      AND deactivation.document_version_id = version.document_version_id
)
UNION ALL
SELECT update_row.epoch_id,
       version.document_version_id,
       version.document_id,
       version.content_hash,
       version.valid_from_epoch,
       version.valid_to_epoch
FROM groundloop_m4_update AS update_row
JOIN groundloop_document_version AS version
  ON version.valid_from_epoch = update_row.epoch_id;

CREATE VIEW groundloop_m4_effective_chunk_version AS
SELECT effective.epoch_id,
       chunk.chunk_version_id,
       chunk.document_version_id,
       chunk.chunk_index,
       chunk.text,
       chunk.text_hash,
       chunk.chunker_version,
       chunk.valid_from_epoch,
       chunk.valid_to_epoch
FROM groundloop_m4_effective_document_version AS effective
JOIN groundloop_chunk_version AS chunk
  ON chunk.document_version_id = effective.document_version_id
 AND chunk.valid_from_epoch = effective.valid_from_epoch
 AND (
     chunk.valid_to_epoch IS NULL
     OR effective.epoch_id < chunk.valid_to_epoch
 );

CREATE VIEW groundloop_m4_effective_candidate_frontier AS
SELECT update_row.epoch_id,
       frontier.claim_id,
       frontier.chunk_version_id,
       frontier.candidate_policy_id,
       frontier.frontier_state,
       frontier.rank,
       frontier.retrieval_score,
       frontier.candidate_artifact_hash,
       frontier.valid_from_epoch,
       frontier.valid_to_epoch
FROM groundloop_m4_update AS update_row
JOIN groundloop_candidate_frontier AS frontier
  ON update_row.previous_published_epoch_id IS NOT NULL
 AND frontier.valid_from_epoch <= update_row.previous_published_epoch_id
 AND (
     frontier.valid_to_epoch IS NULL
     OR update_row.previous_published_epoch_id < frontier.valid_to_epoch
 )
JOIN groundloop_epoch AS creator
  ON creator.epoch_id = frontier.valid_from_epoch
 AND creator.semantic_status = 'sealed'
WHERE NOT EXISTS (
    SELECT 1
    FROM groundloop_candidate_frontier AS working
    WHERE working.valid_from_epoch = update_row.epoch_id
      AND working.claim_id = frontier.claim_id
      AND working.chunk_version_id = frontier.chunk_version_id
      AND working.candidate_policy_id = frontier.candidate_policy_id
)
UNION ALL
SELECT update_row.epoch_id,
       frontier.claim_id,
       frontier.chunk_version_id,
       frontier.candidate_policy_id,
       frontier.frontier_state,
       frontier.rank,
       frontier.retrieval_score,
       frontier.candidate_artifact_hash,
       frontier.valid_from_epoch,
       frontier.valid_to_epoch
FROM groundloop_m4_update AS update_row
JOIN groundloop_candidate_frontier AS frontier
  ON frontier.valid_from_epoch = update_row.epoch_id;

CREATE UNIQUE INDEX groundloop_m4_one_effective_document_version
    ON groundloop_document_version(document_id, valid_from_epoch);
