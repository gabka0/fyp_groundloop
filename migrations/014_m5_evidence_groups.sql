-- GroundLoop M5 semantic core.  This file is not a standalone migration:
-- src/groundloop/postgres/migrations.py executes it with the independent SQL
-- oracle as one immutable, content-hash-ledgered transaction.

-- Freeze every pre-M5 writer surface before checking the live-open predicate.
-- This order is part of the M5-D13 physical contract.
LOCK TABLE groundloop_epoch IN ACCESS EXCLUSIVE MODE;
LOCK TABLE groundloop_m4_update IN ACCESS EXCLUSIVE MODE;
LOCK TABLE groundloop_claim IN ACCESS EXCLUSIVE MODE;
LOCK TABLE groundloop_semantic_observation IN ACCESS EXCLUSIVE MODE;
LOCK TABLE groundloop_observation_currency IN ACCESS EXCLUSIVE MODE;
LOCK TABLE groundloop_published_observation_currency IN ACCESS EXCLUSIVE MODE;
LOCK TABLE groundloop_working_observation_delta IN ACCESS EXCLUSIVE MODE;

DO $groundloop_m5_open_epoch_guard$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM groundloop_epoch
        WHERE structural_status = 'committed'
          AND semantic_status IN ('pending', 'complete')
    ) THEN
        RAISE EXCEPTION
            'M5 core bundle requires no committed pending/complete epoch';
    END IF;
END;
$groundloop_m5_open_epoch_guard$;

CREATE EXTENSION IF NOT EXISTS pgcrypto WITH SCHEMA public;
CREATE EXTENSION IF NOT EXISTS btree_gist WITH SCHEMA public;

CREATE TABLE groundloop_m5_schema_bundle (
    bundle_id text PRIMARY KEY CHECK (btrim(bundle_id) <> ''),
    bundle_sha256 char(64) NOT NULL CHECK (
        bundle_sha256 ~ '^[0-9a-f]{64}$'
    ),
    migration_sha256 char(64) NOT NULL CHECK (
        migration_sha256 ~ '^[0-9a-f]{64}$'
    ),
    oracle_sha256 char(64) NOT NULL CHECK (
        oracle_sha256 ~ '^[0-9a-f]{64}$'
    ),
    prerequisite_sha256 char(64) NOT NULL CHECK (
        prerequisite_sha256 ~ '^[0-9a-f]{64}$'
    ),
    applied_at timestamptz NOT NULL DEFAULT now()
);

COMMENT ON COLUMN groundloop_m5_schema_bundle.prerequisite_sha256 IS
    'SHA-256 identity of the exact local ordered 000-013 source set; the installer separately performs a catalog-shape preflight, and this value is not byte-provenance evidence for a pre-existing database';

CREATE FUNCTION groundloop_m5_reject_immutable_row()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION 'immutable M5 table % cannot be changed', TG_TABLE_NAME;
END;
$$;

CREATE TRIGGER groundloop_m5_schema_bundle_immutable
BEFORE UPDATE OR DELETE ON groundloop_m5_schema_bundle
FOR EACH ROW EXECUTE FUNCTION groundloop_m5_reject_immutable_row();

-- Normalization-v1 recognizes exactly the frozen 29 Unicode code points.
CREATE FUNCTION groundloop_normalize_text_v1(value_to_normalize text)
RETURNS text
LANGUAGE plpgsql
IMMUTABLE
STRICT
PARALLEL SAFE
AS $$
DECLARE
    character text;
    code_point integer;
    normalized text := '';
    have_output boolean := false;
    pending_space boolean := false;
    position integer;
BEGIN
    IF value_to_normalize = '' THEN
        RETURN '';
    END IF;
    FOR position IN 1..char_length(value_to_normalize) LOOP
        character := substr(value_to_normalize, position, 1);
        code_point := ascii(character);
        IF code_point BETWEEN 9 AND 13
           OR code_point BETWEEN 28 AND 32
           OR code_point = 133
           OR code_point = 160
           OR code_point = 5760
           OR code_point BETWEEN 8192 AND 8202
           OR code_point IN (8232, 8233, 8239, 8287, 12288) THEN
            IF have_output THEN
                pending_space := true;
            END IF;
        ELSE
            IF pending_space THEN
                normalized := normalized || ' ';
                pending_space := false;
            END IF;
            normalized := normalized || character;
            have_output := true;
        END IF;
    END LOOP;
    RETURN normalized;
END;
$$;

-- Typed subjects replace the M2 claim-only polymorphic-FK placeholder.
CREATE TABLE groundloop_semantic_subject (
    subject_kind groundloop_subject_kind NOT NULL,
    subject_id text NOT NULL CHECK (btrim(subject_id) <> ''),
    registered_epoch bigint NOT NULL REFERENCES groundloop_epoch(epoch_id),
    PRIMARY KEY (subject_kind, subject_id)
);

INSERT INTO groundloop_semantic_subject (
    subject_kind, subject_id, registered_epoch
)
SELECT 'claim'::groundloop_subject_kind,
       claim.claim_id,
       answer.created_epoch
FROM groundloop_claim AS claim
JOIN groundloop_answer_version AS answer
  ON answer.answer_version_id = claim.answer_version_id
ORDER BY claim.claim_id;

CREATE TRIGGER groundloop_semantic_subject_immutable
BEFORE UPDATE OR DELETE ON groundloop_semantic_subject
FOR EACH ROW EXECUTE FUNCTION groundloop_m5_reject_immutable_row();

CREATE FUNCTION groundloop_m5_register_claim_subject()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    claim_epoch bigint;
BEGIN
    SELECT created_epoch
    INTO STRICT claim_epoch
    FROM groundloop_answer_version
    WHERE answer_version_id = NEW.answer_version_id;

    INSERT INTO groundloop_semantic_subject (
        subject_kind, subject_id, registered_epoch
    ) VALUES ('claim', NEW.claim_id, claim_epoch);
    RETURN NEW;
END;
$$;

CREATE TRIGGER groundloop_claim_registers_semantic_subject
AFTER INSERT ON groundloop_claim
FOR EACH ROW EXECUTE FUNCTION groundloop_m5_register_claim_subject();

ALTER TABLE groundloop_semantic_observation
    DROP CONSTRAINT groundloop_semantic_observation_subject_id_fkey,
    DROP CONSTRAINT groundloop_m2_claim_subject_only,
    ADD COLUMN eligible_for_currency boolean NOT NULL DEFAULT true;

ALTER TABLE groundloop_semantic_observation
    ADD CONSTRAINT groundloop_semantic_observation_typed_subject_fkey
    FOREIGN KEY (subject_kind, subject_id)
    REFERENCES groundloop_semantic_subject(subject_kind, subject_id)
    DEFERRABLE INITIALLY DEFERRED;

CREATE FUNCTION groundloop_m5_assert_observation_eligible(
    observation_id_to_check text
)
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    IF observation_id_to_check IS NOT NULL AND NOT EXISTS (
        SELECT 1
        FROM groundloop_semantic_observation
        WHERE observation_id = observation_id_to_check
          AND eligible_for_currency
    ) THEN
        RAISE EXCEPTION
            'observation % is not eligible for currency', observation_id_to_check;
    END IF;
END;
$$;

CREATE FUNCTION groundloop_m5_validate_currency_holder()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF TG_TABLE_NAME = 'groundloop_working_observation_delta' THEN
        PERFORM groundloop_m5_assert_observation_eligible(NEW.base_observation_id);
        PERFORM groundloop_m5_assert_observation_eligible(NEW.working_observation_id);
    ELSE
        PERFORM groundloop_m5_assert_observation_eligible(NEW.observation_id);
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER groundloop_observation_currency_eligible
BEFORE INSERT OR UPDATE ON groundloop_observation_currency
FOR EACH ROW EXECUTE FUNCTION groundloop_m5_validate_currency_holder();

CREATE TRIGGER groundloop_published_observation_currency_eligible
BEFORE INSERT OR UPDATE ON groundloop_published_observation_currency
FOR EACH ROW EXECUTE FUNCTION groundloop_m5_validate_currency_holder();

CREATE TRIGGER groundloop_working_observation_delta_eligible
BEFORE INSERT OR UPDATE ON groundloop_working_observation_delta
FOR EACH ROW EXECUTE FUNCTION groundloop_m5_validate_currency_holder();

-- The semantic core has its own update/head state. Runtime tables in migration
-- 015 depend on these relations and do not recreate them.
CREATE TABLE groundloop_runtime_mode (
    singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
    mode text NOT NULL CHECK (mode IN ('v1_only', 'm5_active')),
    mode_revision bigint NOT NULL CHECK (mode_revision >= 0),
    updated_at timestamptz NOT NULL DEFAULT now()
);

INSERT INTO groundloop_runtime_mode (
    singleton, mode, mode_revision, updated_at
) VALUES (true, 'v1_only', 0, now());

CREATE TABLE groundloop_m5_publication_head (
    singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
    epoch_id bigint NOT NULL UNIQUE REFERENCES groundloop_epoch(epoch_id),
    sealed_revision bigint NOT NULL CHECK (sealed_revision >= 0),
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE groundloop_m5_activation (
    singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
    activation_id text NOT NULL UNIQUE CHECK (btrim(activation_id) <> ''),
    payload_hash char(64) NOT NULL CHECK (payload_hash ~ '^[0-9a-f]{64}$'),
    base_m4_epoch_id bigint NOT NULL REFERENCES groundloop_epoch(epoch_id),
    activated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TRIGGER groundloop_m5_activation_immutable
BEFORE UPDATE OR DELETE ON groundloop_m5_activation
FOR EACH ROW EXECUTE FUNCTION groundloop_m5_reject_immutable_row();

CREATE FUNCTION groundloop_m5_guard_v1_open()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    current_mode text;
BEGIN
    SELECT mode
    INTO STRICT current_mode
    FROM groundloop_runtime_mode
    WHERE singleton
    FOR UPDATE;
    IF current_mode <> 'v1_only' THEN
        RAISE EXCEPTION 'new M4 v1 mutation epochs are disabled after M5 activation';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER groundloop_m4_update_runtime_mode_guard
BEFORE INSERT ON groundloop_m4_update
FOR EACH ROW EXECUTE FUNCTION groundloop_m5_guard_v1_open();

CREATE TABLE groundloop_m5_update (
    epoch_id bigint PRIMARY KEY REFERENCES groundloop_epoch(epoch_id),
    update_kind text NOT NULL CHECK (
        update_kind IN (
            'register_group', 'replace_group', 'retire_group',
            'observe_requirement', 'policy_change', 'document_insert',
            'document_delete', 'document_replace'
        )
    ),
    previous_published_epoch_id bigint NOT NULL REFERENCES groundloop_epoch(epoch_id),
    decision_policy_version text NOT NULL
        REFERENCES groundloop_decision_policy(policy_version),
    manifest jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at timestamptz NOT NULL DEFAULT now(),
    CHECK (epoch_id <> previous_published_epoch_id)
);

CREATE FUNCTION groundloop_m5_guard_typed_open()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    current_mode text;
    m4_head bigint;
    m5_head bigint;
BEGIN
    SELECT mode
    INTO STRICT current_mode
    FROM groundloop_runtime_mode
    WHERE singleton
    FOR UPDATE;
    IF current_mode <> 'm5_active' THEN
        RAISE EXCEPTION 'typed M5 mutation requires activated runtime mode';
    END IF;

    SELECT epoch_id
    INTO STRICT m4_head
    FROM groundloop_m4_publication_head
    WHERE singleton
    FOR UPDATE;
    SELECT epoch_id
    INTO STRICT m5_head
    FROM groundloop_m5_publication_head
    WHERE singleton
    FOR UPDATE;
    IF m4_head <> m5_head OR NEW.previous_published_epoch_id <> m5_head THEN
        RAISE EXCEPTION 'M4/M5 publication heads are not equal to the typed base';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER groundloop_m5_update_runtime_mode_guard
BEFORE INSERT ON groundloop_m5_update
FOR EACH ROW EXECUTE FUNCTION groundloop_m5_guard_typed_open();

CREATE TABLE groundloop_m5_group_family (
    group_family_id text PRIMARY KEY CHECK (btrim(group_family_id) <> ''),
    claim_id text NOT NULL REFERENCES groundloop_claim(claim_id),
    creator_epoch_id bigint NOT NULL REFERENCES groundloop_epoch(epoch_id),
    lifecycle_state text NOT NULL CHECK (
        lifecycle_state IN ('STAGED', 'PUBLISHED', 'FAILED')
    )
);

CREATE TABLE groundloop_m5_group_version (
    group_version_id text PRIMARY KEY CHECK (btrim(group_version_id) <> ''),
    group_family_id text NOT NULL
        REFERENCES groundloop_m5_group_family(group_family_id)
        DEFERRABLE INITIALLY DEFERRED,
    creator_epoch_id bigint NOT NULL REFERENCES groundloop_epoch(epoch_id),
    lifecycle_state text NOT NULL CHECK (
        lifecycle_state IN ('STAGED', 'PUBLISHED', 'FAILED')
    ),
    group_type text NOT NULL CHECK (group_type = 'support_conjunction'),
    construction_kind text NOT NULL CHECK (
        construction_kind IN ('gold', 'controlled', 'model_proposed')
    ),
    construction_source_id text NOT NULL CHECK (btrim(construction_source_id) <> ''),
    constructor_model_id text,
    constructor_model_version text,
    constructor_prompt_version text,
    supersedes_group_version_id text
        REFERENCES groundloop_m5_group_version(group_version_id)
        DEFERRABLE INITIALLY DEFERRED,
    semantic_structure_hash char(64) NOT NULL CHECK (
        semantic_structure_hash ~ '^[0-9a-f]{64}$'
    ),
    record_payload_hash char(64) NOT NULL CHECK (
        record_payload_hash ~ '^[0-9a-f]{64}$'
    ),
    UNIQUE (group_version_id, group_family_id),
    CHECK (supersedes_group_version_id IS DISTINCT FROM group_version_id),
    CHECK (
        (construction_kind IN ('gold', 'controlled')
         AND constructor_model_id IS NULL
         AND constructor_model_version IS NULL
         AND constructor_prompt_version IS NULL)
        OR
        (construction_kind = 'model_proposed'
         AND btrim(constructor_model_id) <> ''
         AND btrim(constructor_model_version) <> ''
         AND btrim(constructor_prompt_version) <> '')
    )
);

CREATE UNIQUE INDEX groundloop_m5_one_published_group_successor
    ON groundloop_m5_group_version(supersedes_group_version_id)
    WHERE lifecycle_state = 'PUBLISHED'
      AND supersedes_group_version_id IS NOT NULL;

CREATE TABLE groundloop_m5_requirement_version (
    requirement_version_id text PRIMARY KEY CHECK (
        btrim(requirement_version_id) <> ''
    ),
    group_version_id text NOT NULL
        REFERENCES groundloop_m5_group_version(group_version_id)
        DEFERRABLE INITIALLY DEFERRED,
    creator_epoch_id bigint NOT NULL REFERENCES groundloop_epoch(epoch_id),
    lifecycle_state text NOT NULL CHECK (
        lifecycle_state IN ('STAGED', 'PUBLISHED', 'FAILED')
    ),
    ordinal integer NOT NULL CHECK (ordinal BETWEEN 0 AND 7),
    requirement_text text NOT NULL CHECK (
        requirement_text <> ''
        AND requirement_text = groundloop_normalize_text_v1(requirement_text)
    ),
    requirement_text_hash char(64) NOT NULL CHECK (
        requirement_text_hash ~ '^[0-9a-f]{64}$'
        AND requirement_text_hash = encode(
            digest(convert_to(requirement_text, 'UTF8'), 'sha256'), 'hex'
        )
    ),
    constructor_model_id text,
    constructor_model_version text,
    constructor_prompt_version text,
    supersedes_requirement_version_id text
        REFERENCES groundloop_m5_requirement_version(requirement_version_id)
        DEFERRABLE INITIALLY DEFERRED,
    UNIQUE (group_version_id, ordinal),
    UNIQUE (group_version_id, requirement_text_hash),
    CHECK (
        supersedes_requirement_version_id IS DISTINCT FROM requirement_version_id
    )
);

CREATE UNIQUE INDEX groundloop_m5_one_published_requirement_successor
    ON groundloop_m5_requirement_version(supersedes_requirement_version_id)
    WHERE lifecycle_state = 'PUBLISHED'
      AND supersedes_requirement_version_id IS NOT NULL;

CREATE FUNCTION groundloop_m5_register_requirement_subject()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO groundloop_semantic_subject (
        subject_kind, subject_id, registered_epoch
    ) VALUES ('requirement', NEW.requirement_version_id, NEW.creator_epoch_id);
    RETURN NEW;
END;
$$;

CREATE TRIGGER groundloop_m5_requirement_registers_semantic_subject
AFTER INSERT ON groundloop_m5_requirement_version
FOR EACH ROW EXECUTE FUNCTION groundloop_m5_register_requirement_subject();

CREATE FUNCTION groundloop_m5_validate_semantic_subject_subtype()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF NEW.subject_kind = 'claim' AND NOT EXISTS (
        SELECT 1 FROM groundloop_claim WHERE claim_id = NEW.subject_id
    ) THEN
        RAISE EXCEPTION 'CLAIM semantic subject % has no claim subtype', NEW.subject_id;
    ELSIF NEW.subject_kind = 'requirement' AND NOT EXISTS (
        SELECT 1
        FROM groundloop_m5_requirement_version
        WHERE requirement_version_id = NEW.subject_id
    ) THEN
        RAISE EXCEPTION
            'REQUIREMENT semantic subject % has no requirement subtype', NEW.subject_id;
    END IF;
    RETURN NULL;
END;
$$;

CREATE CONSTRAINT TRIGGER groundloop_semantic_subject_subtype_integrity
AFTER INSERT ON groundloop_semantic_subject
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION groundloop_m5_validate_semantic_subject_subtype();

CREATE TABLE groundloop_m5_group_validity (
    group_version_id text PRIMARY KEY
        REFERENCES groundloop_m5_group_version(group_version_id)
        DEFERRABLE INITIALLY DEFERRED,
    group_family_id text NOT NULL
        REFERENCES groundloop_m5_group_family(group_family_id)
        DEFERRABLE INITIALLY DEFERRED,
    claim_id text NOT NULL REFERENCES groundloop_claim(claim_id),
    semantic_structure_hash char(64) NOT NULL CHECK (
        semantic_structure_hash ~ '^[0-9a-f]{64}$'
    ),
    supersedes_group_version_id text,
    valid_from_epoch bigint NOT NULL REFERENCES groundloop_epoch(epoch_id),
    valid_to_epoch bigint REFERENCES groundloop_epoch(epoch_id),
    FOREIGN KEY (group_version_id, group_family_id)
        REFERENCES groundloop_m5_group_version(group_version_id, group_family_id)
        DEFERRABLE INITIALLY DEFERRED,
    CHECK (valid_to_epoch IS NULL OR valid_to_epoch > valid_from_epoch)
);

ALTER TABLE groundloop_m5_group_validity
    ADD CONSTRAINT groundloop_m5_group_family_validity_no_overlap
    EXCLUDE USING gist (
        group_family_id WITH =,
        int8range(valid_from_epoch, valid_to_epoch, '[)') WITH &&
    ) DEFERRABLE INITIALLY DEFERRED;

ALTER TABLE groundloop_m5_group_validity
    ADD CONSTRAINT groundloop_m5_group_semantic_validity_no_overlap
    EXCLUDE USING gist (
        claim_id WITH =,
        semantic_structure_hash WITH =,
        int8range(valid_from_epoch, valid_to_epoch, '[)') WITH &&
    ) DEFERRABLE INITIALLY DEFERRED;

CREATE UNIQUE INDEX groundloop_m5_one_published_validity_successor
    ON groundloop_m5_group_validity(supersedes_group_version_id)
    WHERE supersedes_group_version_id IS NOT NULL;

CREATE TABLE groundloop_m5_group_deactivation (
    epoch_id bigint NOT NULL REFERENCES groundloop_m5_update(epoch_id),
    group_version_id text NOT NULL
        REFERENCES groundloop_m5_group_version(group_version_id),
    action text NOT NULL CHECK (action IN ('REPLACE', 'RETIRE')),
    successor_group_version_id text
        REFERENCES groundloop_m5_group_version(group_version_id)
        DEFERRABLE INITIALLY DEFERRED,
    event_id text NOT NULL REFERENCES groundloop_epoch(event_id),
    PRIMARY KEY (epoch_id, group_version_id),
    CHECK (
        (action = 'REPLACE' AND successor_group_version_id IS NOT NULL)
        OR (action = 'RETIRE' AND successor_group_version_id IS NULL)
    )
);

CREATE TABLE groundloop_m5_group_family_retirement (
    group_family_id text PRIMARY KEY
        REFERENCES groundloop_m5_group_family(group_family_id),
    retired_epoch_id bigint NOT NULL REFERENCES groundloop_epoch(epoch_id),
    event_id text NOT NULL UNIQUE REFERENCES groundloop_epoch(event_id)
);

CREATE TRIGGER groundloop_m5_group_deactivation_immutable
BEFORE UPDATE OR DELETE ON groundloop_m5_group_deactivation
FOR EACH ROW EXECUTE FUNCTION groundloop_m5_reject_immutable_row();

CREATE TRIGGER groundloop_m5_group_family_retirement_immutable
BEFORE UPDATE OR DELETE ON groundloop_m5_group_family_retirement
FOR EACH ROW EXECUTE FUNCTION groundloop_m5_reject_immutable_row();

CREATE FUNCTION groundloop_m5_validate_lifecycle_change()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'M5 lifecycle rows cannot be deleted';
    END IF;
    IF (to_jsonb(NEW) - 'lifecycle_state')
       IS DISTINCT FROM (to_jsonb(OLD) - 'lifecycle_state') THEN
        RAISE EXCEPTION 'M5 immutable semantic fields cannot change';
    END IF;
    IF OLD.lifecycle_state <> 'STAGED'
       OR NEW.lifecycle_state NOT IN ('PUBLISHED', 'FAILED') THEN
        RAISE EXCEPTION 'invalid M5 lifecycle transition % -> %',
            OLD.lifecycle_state, NEW.lifecycle_state;
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER groundloop_m5_group_family_lifecycle_guard
BEFORE UPDATE OR DELETE ON groundloop_m5_group_family
FOR EACH ROW EXECUTE FUNCTION groundloop_m5_validate_lifecycle_change();

CREATE TRIGGER groundloop_m5_group_version_lifecycle_guard
BEFORE UPDATE OR DELETE ON groundloop_m5_group_version
FOR EACH ROW EXECUTE FUNCTION groundloop_m5_validate_lifecycle_change();

CREATE TRIGGER groundloop_m5_requirement_version_lifecycle_guard
BEFORE UPDATE OR DELETE ON groundloop_m5_requirement_version
FOR EACH ROW EXECUTE FUNCTION groundloop_m5_validate_lifecycle_change();

CREATE FUNCTION groundloop_m5_validate_group_validity_change()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'published M5 group validity cannot be deleted';
    END IF;
    IF (to_jsonb(NEW) - 'valid_to_epoch')
       IS DISTINCT FROM (to_jsonb(OLD) - 'valid_to_epoch')
       OR OLD.valid_to_epoch IS NOT NULL
       OR NEW.valid_to_epoch IS NULL
       OR NEW.valid_to_epoch <= OLD.valid_from_epoch THEN
        RAISE EXCEPTION 'M5 group validity may close exactly once';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER groundloop_m5_group_validity_close_once
BEFORE UPDATE OR DELETE ON groundloop_m5_group_validity
FOR EACH ROW EXECUTE FUNCTION groundloop_m5_validate_group_validity_change();

CREATE FUNCTION groundloop_m5_assert_whole_group_integrity()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM groundloop_m5_group_version AS group_row
        LEFT JOIN groundloop_m5_requirement_version AS requirement
          ON requirement.group_version_id = group_row.group_version_id
        GROUP BY group_row.group_version_id, group_row.lifecycle_state
        HAVING count(requirement.requirement_version_id) NOT BETWEEN 1 AND 8
            OR min(requirement.ordinal) <> 0
            OR max(requirement.ordinal) <> count(requirement.requirement_version_id) - 1
            OR count(DISTINCT requirement.ordinal)
               <> count(requirement.requirement_version_id)
            OR bool_or(requirement.lifecycle_state <> group_row.lifecycle_state)
    ) THEN
        RAISE EXCEPTION 'M5 groups require one-to-eight dense lifecycle-aligned requirements';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM groundloop_m5_requirement_version AS requirement
        JOIN groundloop_m5_group_version AS group_row
          ON group_row.group_version_id = requirement.group_version_id
        WHERE (requirement.constructor_model_id,
               requirement.constructor_model_version,
               requirement.constructor_prompt_version)
              IS DISTINCT FROM
              (group_row.constructor_model_id,
               group_row.constructor_model_version,
               group_row.constructor_prompt_version)
    ) THEN
        RAISE EXCEPTION 'M5 requirement provenance differs from its group';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM groundloop_m5_group_version AS group_row
        WHERE group_row.semantic_structure_hash <>
              groundloop_m5_expected_semantic_structure(group_row.group_version_id)
           OR group_row.record_payload_hash <>
              groundloop_m5_expected_group_record(group_row.group_version_id)
    ) THEN
        RAISE EXCEPTION 'M5 group semantic or record digest is invalid';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM groundloop_m5_group_validity AS validity
        JOIN groundloop_m5_group_version AS group_row
          ON group_row.group_version_id = validity.group_version_id
        JOIN groundloop_m5_group_family AS family
          ON family.group_family_id = group_row.group_family_id
        WHERE group_row.lifecycle_state <> 'PUBLISHED'
           OR family.lifecycle_state <> 'PUBLISHED'
           OR validity.group_family_id <> family.group_family_id
           OR validity.claim_id <> family.claim_id
           OR validity.semantic_structure_hash <> group_row.semantic_structure_hash
           OR validity.supersedes_group_version_id
              IS DISTINCT FROM group_row.supersedes_group_version_id
           OR validity.valid_from_epoch <> group_row.creator_epoch_id
    ) THEN
        RAISE EXCEPTION 'published M5 validity disagrees with immutable group identity';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM groundloop_m5_group_validity AS successor
        JOIN groundloop_m5_group_validity AS predecessor
          ON predecessor.group_version_id = successor.supersedes_group_version_id
        WHERE predecessor.group_family_id <> successor.group_family_id
           OR predecessor.valid_to_epoch IS DISTINCT FROM successor.valid_from_epoch
    ) THEN
        RAISE EXCEPTION 'published M5 successor is not adjacent in its family';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM groundloop_m5_requirement_version AS successor
        JOIN groundloop_m5_requirement_version AS predecessor
          ON predecessor.requirement_version_id =
             successor.supersedes_requirement_version_id
        JOIN groundloop_m5_group_version AS successor_group
          ON successor_group.group_version_id = successor.group_version_id
        WHERE predecessor.group_version_id IS DISTINCT FROM
              successor_group.supersedes_group_version_id
    ) THEN
        RAISE EXCEPTION 'M5 requirement predecessor is not in the prior group version';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM groundloop_m5_group_version AS successor
        JOIN groundloop_m5_group_version AS predecessor
          ON predecessor.group_version_id =
             successor.supersedes_group_version_id
        WHERE predecessor.group_family_id <> successor.group_family_id
    ) THEN
        RAISE EXCEPTION 'M5 group predecessor is not in the same family';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM groundloop_m5_group_version AS successor
        WHERE successor.supersedes_group_version_id IS NOT NULL
          AND successor.lifecycle_state IN ('STAGED', 'PUBLISHED')
        GROUP BY successor.supersedes_group_version_id
        HAVING count(*) > 1
    ) OR EXISTS (
        SELECT 1
        FROM groundloop_m5_requirement_version AS successor
        WHERE successor.supersedes_requirement_version_id IS NOT NULL
          AND successor.lifecycle_state IN ('STAGED', 'PUBLISHED')
        GROUP BY successor.supersedes_requirement_version_id
        HAVING count(*) > 1
    ) THEN
        RAISE EXCEPTION 'M5 active lineage contains a fork';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM groundloop_m5_group_version AS staged
        WHERE staged.lifecycle_state = 'STAGED'
          AND (
              NOT EXISTS (
                  SELECT 1
                  FROM groundloop_m5_update AS update_row
                  WHERE update_row.epoch_id = staged.creator_epoch_id
              )
              OR (
                  staged.supersedes_group_version_id IS NOT NULL
                  AND NOT EXISTS (
                      SELECT 1
                      FROM groundloop_m5_group_deactivation AS deactivation
                      WHERE deactivation.epoch_id = staged.creator_epoch_id
                        AND deactivation.action = 'REPLACE'
                        AND deactivation.group_version_id =
                            staged.supersedes_group_version_id
                        AND deactivation.successor_group_version_id =
                            staged.group_version_id
                  )
              )
          )
    ) THEN
        RAISE EXCEPTION 'staged M5 group is not owned by its structural update';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM groundloop_m5_effective_group_version AS effective
        GROUP BY effective.epoch_id,
                 effective.claim_id,
                 effective.semantic_structure_hash
        HAVING count(*) > 1
    ) THEN
        RAISE EXCEPTION 'effective M5 snapshot contains a semantic duplicate';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM groundloop_m5_group_family_retirement AS retirement
        JOIN groundloop_m5_group_version AS group_row
          ON group_row.group_family_id = retirement.group_family_id
        LEFT JOIN groundloop_m5_group_validity AS validity
          ON validity.group_version_id = group_row.group_version_id
        WHERE group_row.lifecycle_state = 'STAGED'
           OR (
               group_row.lifecycle_state = 'PUBLISHED'
               AND validity.valid_from_epoch >= retirement.retired_epoch_id
           )
           OR (
               group_row.lifecycle_state = 'PUBLISHED'
               AND validity.valid_to_epoch IS DISTINCT FROM retirement.retired_epoch_id
               AND validity.valid_from_epoch < retirement.retired_epoch_id
               AND (
                   validity.valid_to_epoch IS NULL
                   OR retirement.retired_epoch_id < validity.valid_to_epoch
               )
           )
    ) THEN
        RAISE EXCEPTION 'retired M5 family has a staged or nonclosed published version';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM groundloop_m5_group_deactivation AS deactivation
        JOIN groundloop_m5_group_version AS old_group
          ON old_group.group_version_id = deactivation.group_version_id
        JOIN groundloop_epoch AS epoch
          ON epoch.epoch_id = deactivation.epoch_id
        LEFT JOIN groundloop_m5_group_version AS successor
          ON successor.group_version_id = deactivation.successor_group_version_id
        LEFT JOIN groundloop_m5_group_validity AS old_validity
          ON old_validity.group_version_id = old_group.group_version_id
        LEFT JOIN groundloop_m5_group_validity AS successor_validity
          ON successor_validity.group_version_id = successor.group_version_id
        LEFT JOIN groundloop_m5_group_family_retirement AS retirement
          ON retirement.group_family_id = old_group.group_family_id
         AND retirement.retired_epoch_id = deactivation.epoch_id
        WHERE deactivation.event_id <> epoch.event_id
           OR old_group.lifecycle_state <> 'PUBLISHED'
           OR (deactivation.action = 'REPLACE' AND (
               successor.creator_epoch_id <> deactivation.epoch_id
               OR successor.group_family_id <> old_group.group_family_id
               OR successor.supersedes_group_version_id <> old_group.group_version_id
               OR (
                   epoch.semantic_status IN ('pending', 'complete')
                   AND successor.lifecycle_state <> 'STAGED'
               )
               OR (
                   epoch.semantic_status IN ('failed', 'degraded')
                   AND successor.lifecycle_state <> 'FAILED'
               )
               OR (
                   epoch.semantic_status = 'sealed'
                   AND (
                       successor.lifecycle_state <> 'PUBLISHED'
                       OR successor_validity.valid_from_epoch IS DISTINCT FROM
                          deactivation.epoch_id
                       OR old_validity.valid_to_epoch IS DISTINCT FROM
                          deactivation.epoch_id
                   )
               )
           ))
           OR (deactivation.action = 'RETIRE' AND (
               (
                   epoch.semantic_status IN ('pending', 'complete', 'failed', 'degraded')
                   AND retirement.group_family_id IS NOT NULL
               )
               OR (
                   epoch.semantic_status = 'sealed'
                   AND (
                       retirement.group_family_id IS NULL
                       OR retirement.event_id <> epoch.event_id
                       OR old_validity.valid_to_epoch IS DISTINCT FROM
                          deactivation.epoch_id
                   )
               )
           ))
    ) THEN
        RAISE EXCEPTION 'invalid M5 deactivation overlay';
    END IF;
    RETURN NULL;
END;
$$;

CREATE CONSTRAINT TRIGGER groundloop_m5_group_family_integrity
AFTER INSERT OR UPDATE OR DELETE ON groundloop_m5_group_family
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION groundloop_m5_assert_whole_group_integrity();

CREATE CONSTRAINT TRIGGER groundloop_m5_group_version_integrity
AFTER INSERT OR UPDATE OR DELETE ON groundloop_m5_group_version
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION groundloop_m5_assert_whole_group_integrity();

CREATE CONSTRAINT TRIGGER groundloop_m5_requirement_version_integrity
AFTER INSERT OR UPDATE OR DELETE ON groundloop_m5_requirement_version
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION groundloop_m5_assert_whole_group_integrity();

CREATE CONSTRAINT TRIGGER groundloop_m5_group_validity_integrity
AFTER INSERT OR UPDATE OR DELETE ON groundloop_m5_group_validity
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION groundloop_m5_assert_whole_group_integrity();

CREATE CONSTRAINT TRIGGER groundloop_m5_group_deactivation_integrity
AFTER INSERT OR UPDATE OR DELETE ON groundloop_m5_group_deactivation
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION groundloop_m5_assert_whole_group_integrity();

CREATE CONSTRAINT TRIGGER groundloop_m5_group_retirement_integrity
AFTER INSERT OR UPDATE OR DELETE ON groundloop_m5_group_family_retirement
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION groundloop_m5_assert_whole_group_integrity();

CREATE VIEW groundloop_m5_effective_group_version AS
SELECT update_row.epoch_id,
       group_row.group_version_id,
       group_row.group_family_id,
       family.claim_id,
       group_row.group_type,
       group_row.construction_kind,
       group_row.construction_source_id,
       group_row.semantic_structure_hash,
       group_row.record_payload_hash,
       false AS staged
FROM groundloop_m5_update AS update_row
JOIN groundloop_m5_group_validity AS validity
  ON validity.valid_from_epoch <= update_row.previous_published_epoch_id
 AND (
     validity.valid_to_epoch IS NULL
     OR update_row.previous_published_epoch_id < validity.valid_to_epoch
 )
JOIN groundloop_m5_group_version AS group_row
  ON group_row.group_version_id = validity.group_version_id
JOIN groundloop_m5_group_family AS family
  ON family.group_family_id = group_row.group_family_id
WHERE NOT EXISTS (
    SELECT 1
    FROM groundloop_m5_group_deactivation AS deactivation
    WHERE deactivation.epoch_id = update_row.epoch_id
      AND deactivation.group_version_id = group_row.group_version_id
)
UNION ALL
SELECT update_row.epoch_id,
       group_row.group_version_id,
       group_row.group_family_id,
       family.claim_id,
       group_row.group_type,
       group_row.construction_kind,
       group_row.construction_source_id,
       group_row.semantic_structure_hash,
       group_row.record_payload_hash,
       true AS staged
FROM groundloop_m5_update AS update_row
JOIN groundloop_m5_group_version AS group_row
  ON group_row.creator_epoch_id = update_row.epoch_id
 AND group_row.lifecycle_state = 'STAGED'
JOIN groundloop_m5_group_family AS family
  ON family.group_family_id = group_row.group_family_id;

CREATE VIEW groundloop_m5_effective_requirement_version AS
SELECT effective.epoch_id,
       requirement.requirement_version_id,
       requirement.group_version_id,
       requirement.ordinal,
       requirement.requirement_text,
       requirement.requirement_text_hash,
       effective.claim_id,
       effective.staged
FROM groundloop_m5_effective_group_version AS effective
JOIN groundloop_m5_requirement_version AS requirement
  ON requirement.group_version_id = effective.group_version_id
 AND requirement.lifecycle_state = CASE
     WHEN effective.staged THEN 'STAGED'
     ELSE 'PUBLISHED'
 END;

CREATE INDEX groundloop_m5_group_family_by_claim
    ON groundloop_m5_group_family(claim_id, group_family_id);
CREATE INDEX groundloop_m5_group_versions_by_family
    ON groundloop_m5_group_version(group_family_id, creator_epoch_id);
CREATE INDEX groundloop_m5_requirements_by_group
    ON groundloop_m5_requirement_version(group_version_id, ordinal);
CREATE INDEX groundloop_m5_requirements_by_text_hash
    ON groundloop_m5_requirement_version(requirement_text_hash, requirement_version_id);
CREATE INDEX groundloop_m5_group_validity_by_epoch
    ON groundloop_m5_group_validity(valid_from_epoch, valid_to_epoch);

-- Epoch-local typed currency history. NULL observation_id is an explicit
-- tombstone and suppresses the previous published holder at that revision.
CREATE TABLE groundloop_m5_working_currency_history (
    epoch_id bigint NOT NULL REFERENCES groundloop_m5_update(epoch_id),
    subject_kind groundloop_subject_kind NOT NULL,
    subject_id text NOT NULL,
    chunk_version_id text NOT NULL,
    task_type text NOT NULL,
    observation_id text,
    valid_from_revision bigint NOT NULL CHECK (valid_from_revision >= 0),
    valid_to_revision bigint,
    PRIMARY KEY (
        epoch_id, subject_kind, subject_id, chunk_version_id, task_type,
        valid_from_revision
    ),
    FOREIGN KEY (subject_kind, subject_id)
        REFERENCES groundloop_semantic_subject(subject_kind, subject_id)
        DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (
        observation_id, subject_kind, subject_id, chunk_version_id, task_type
    ) REFERENCES groundloop_semantic_observation (
        observation_id, subject_kind, subject_id, chunk_version_id, task_type
    ) DEFERRABLE INITIALLY DEFERRED,
    CHECK (
        valid_to_revision IS NULL
        OR valid_to_revision > valid_from_revision
    )
);

ALTER TABLE groundloop_m5_working_currency_history
    ADD CONSTRAINT groundloop_m5_working_currency_no_overlap
    EXCLUDE USING gist (
        epoch_id WITH =,
        subject_kind WITH =,
        subject_id WITH =,
        chunk_version_id WITH =,
        task_type WITH =,
        int8range(valid_from_revision, valid_to_revision, '[)') WITH &&
    ) DEFERRABLE INITIALLY DEFERRED;

CREATE UNIQUE INDEX groundloop_m5_one_open_working_currency
    ON groundloop_m5_working_currency_history (
        epoch_id, subject_kind, subject_id, chunk_version_id, task_type
    ) WHERE valid_to_revision IS NULL;

CREATE INDEX groundloop_m5_working_currency_observation
    ON groundloop_m5_working_currency_history(observation_id)
    WHERE observation_id IS NOT NULL;

CREATE FUNCTION groundloop_m5_assert_writable_epoch(epoch_id_to_check bigint)
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
    structural text;
    semantic text;
BEGIN
    SELECT structural_status, semantic_status
    INTO STRICT structural, semantic
    FROM groundloop_epoch
    WHERE epoch_id = epoch_id_to_check;
    IF structural <> 'committed' OR semantic NOT IN ('pending', 'complete') THEN
        RAISE EXCEPTION 'terminal or unopened M5 epoch % cannot change', epoch_id_to_check;
    END IF;
END;
$$;

CREATE FUNCTION groundloop_m5_validate_revision_interval_mutation()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    current_revision bigint;
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'M5 revision history cannot be deleted';
    END IF;
    PERFORM groundloop_m5_assert_writable_epoch(NEW.epoch_id);
    SELECT revision
    INTO STRICT current_revision
    FROM groundloop_epoch
    WHERE epoch_id = NEW.epoch_id;
    IF TG_OP = 'INSERT' AND NEW.valid_from_revision <> current_revision THEN
        RAISE EXCEPTION 'M5 revision history must open at the current revision';
    END IF;
    IF TG_OP = 'UPDATE' THEN
        IF (to_jsonb(NEW) - 'valid_to_revision')
           IS DISTINCT FROM (to_jsonb(OLD) - 'valid_to_revision')
           OR OLD.valid_to_revision IS NOT NULL
           OR NEW.valid_to_revision IS NULL
           OR NEW.valid_to_revision <= OLD.valid_from_revision
           OR NEW.valid_to_revision <> current_revision THEN
            RAISE EXCEPTION 'M5 revision history may close exactly once';
        END IF;
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER groundloop_m5_working_currency_close_once
BEFORE INSERT OR UPDATE OR DELETE ON groundloop_m5_working_currency_history
FOR EACH ROW EXECUTE FUNCTION groundloop_m5_validate_revision_interval_mutation();

CREATE TRIGGER groundloop_m5_working_currency_eligible
BEFORE INSERT OR UPDATE ON groundloop_m5_working_currency_history
FOR EACH ROW EXECUTE FUNCTION groundloop_m5_validate_currency_holder();

CREATE FUNCTION groundloop_m5_assert_currency_chain()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF EXISTS (
        WITH ordered AS (
            SELECT epoch_id,
                   subject_kind,
                   subject_id,
                   chunk_version_id,
                   task_type,
                   valid_from_revision,
                   valid_to_revision,
                   lead(valid_from_revision) OVER (
                       PARTITION BY epoch_id, subject_kind, subject_id,
                                    chunk_version_id, task_type
                       ORDER BY valid_from_revision
                   ) AS next_from
            FROM groundloop_m5_working_currency_history
        )
        SELECT 1
        FROM ordered
        WHERE (next_from IS NOT NULL AND valid_to_revision IS DISTINCT FROM next_from)
           OR (next_from IS NULL AND valid_to_revision IS NOT NULL)
    ) THEN
        RAISE EXCEPTION 'M5 working currency history must be contiguous and open-ended';
    END IF;
    RETURN NULL;
END;
$$;

CREATE CONSTRAINT TRIGGER groundloop_m5_working_currency_chain
AFTER INSERT OR UPDATE OR DELETE ON groundloop_m5_working_currency_history
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION groundloop_m5_assert_currency_chain();

CREATE FUNCTION groundloop_m5_currency_at(
    epoch_id_to_read bigint,
    revision_to_read bigint
)
RETURNS TABLE (
    subject_kind groundloop_subject_kind,
    subject_id text,
    chunk_version_id text,
    task_type text,
    observation_id text
)
LANGUAGE sql
STABLE
STRICT
AS $$
    WITH event AS (
        SELECT previous_published_epoch_id
        FROM groundloop_m5_update
        WHERE epoch_id = epoch_id_to_read
    ),
    working AS (
        SELECT history.subject_kind,
               history.subject_id,
               history.chunk_version_id,
               history.task_type,
               history.observation_id
        FROM groundloop_m5_working_currency_history AS history
        WHERE history.epoch_id = epoch_id_to_read
          AND history.valid_from_revision <= revision_to_read
          AND (
              history.valid_to_revision IS NULL
              OR revision_to_read < history.valid_to_revision
          )
    )
    SELECT working.subject_kind,
           working.subject_id,
           working.chunk_version_id,
           working.task_type,
           working.observation_id
    FROM working
    UNION ALL
    SELECT published.subject_kind,
           published.subject_id,
           published.chunk_version_id,
           published.task_type,
           published.observation_id
    FROM event
    JOIN groundloop_published_observation_currency AS published
      ON published.valid_from_epoch <= event.previous_published_epoch_id
     AND (
         published.valid_to_epoch IS NULL
         OR event.previous_published_epoch_id < published.valid_to_epoch
     )
    WHERE NOT EXISTS (
        SELECT 1
        FROM working
        WHERE working.subject_kind = published.subject_kind
          AND working.subject_id = published.subject_id
          AND working.chunk_version_id = published.chunk_version_id
          AND working.task_type = published.task_type
    );
$$;

CREATE FUNCTION groundloop_m5_sorted_unique_text_array(value_to_check text[])
RETURNS boolean
LANGUAGE sql
IMMUTABLE
STRICT
PARALLEL SAFE
AS $$
    SELECT NOT EXISTS (
               SELECT 1
               FROM unnest(value_to_check) AS value
               WHERE value IS NULL OR btrim(value) = ''
           )
       AND value_to_check = coalesce(
               (
                   SELECT array_agg(
                       DISTINCT value COLLATE "C" ORDER BY value COLLATE "C"
                   )
                   FROM unnest(value_to_check) AS value
               ),
               ARRAY[]::text[]
           );
$$;

CREATE FUNCTION groundloop_m5_sorted_unique_sha256_array(value_to_check text[])
RETURNS boolean
LANGUAGE sql
IMMUTABLE
STRICT
PARALLEL SAFE
AS $$
    SELECT groundloop_m5_sorted_unique_text_array(value_to_check)
       AND NOT EXISTS (
               SELECT 1
               FROM unnest(value_to_check) AS value
               WHERE value !~ '^[0-9a-f]{64}$'
           );
$$;

CREATE TABLE groundloop_m5_working_requirement_state (
    epoch_id bigint NOT NULL REFERENCES groundloop_m5_update(epoch_id),
    requirement_version_id text NOT NULL
        REFERENCES groundloop_m5_requirement_version(requirement_version_id),
    witness_hashes text[] NOT NULL CHECK (
        groundloop_m5_sorted_unique_sha256_array(witness_hashes)
        AND cardinality(witness_hashes) <= 2147483647
    ),
    supporting_observation_ids text[] NOT NULL CHECK (
        groundloop_m5_sorted_unique_text_array(supporting_observation_ids)
    ),
    witness_count integer NOT NULL CHECK (
        witness_count >= 0 AND witness_count = cardinality(witness_hashes)
    ),
    satisfied boolean NOT NULL CHECK (satisfied = (witness_count > 0)),
    decision_policy_version text NOT NULL
        REFERENCES groundloop_decision_policy(policy_version),
    updated_revision bigint NOT NULL CHECK (updated_revision >= 0),
    PRIMARY KEY (epoch_id, requirement_version_id)
);

CREATE TABLE groundloop_m5_working_group_state (
    epoch_id bigint NOT NULL REFERENCES groundloop_m5_update(epoch_id),
    group_version_id text NOT NULL
        REFERENCES groundloop_m5_group_version(group_version_id),
    requirement_count integer NOT NULL CHECK (requirement_count BETWEEN 1 AND 8),
    satisfied_count integer NOT NULL CHECK (
        satisfied_count BETWEEN 0 AND requirement_count
    ),
    matching_size integer NOT NULL CHECK (
        matching_size BETWEEN 0 AND requirement_count
    ),
    complete boolean NOT NULL CHECK (
        complete = (matching_size = requirement_count)
    ),
    decision_policy_version text NOT NULL
        REFERENCES groundloop_decision_policy(policy_version),
    certificate_digest char(64),
    updated_revision bigint NOT NULL CHECK (updated_revision >= 0),
    PRIMARY KEY (epoch_id, group_version_id),
    CHECK (
        (complete AND certificate_digest ~ '^[0-9a-f]{64}$')
        OR (NOT complete AND certificate_digest IS NULL)
    )
);

CREATE TABLE groundloop_m5_working_claim_state (
    epoch_id bigint NOT NULL REFERENCES groundloop_m5_update(epoch_id),
    claim_id text NOT NULL REFERENCES groundloop_claim(claim_id),
    support_count integer NOT NULL CHECK (support_count >= 0),
    refute_count integer NOT NULL CHECK (refute_count >= 0),
    best_support_score double precision,
    best_refute_score double precision,
    supporting_observation_ids text[] NOT NULL CHECK (
        groundloop_m5_sorted_unique_text_array(supporting_observation_ids)
    ),
    refuting_observation_ids text[] NOT NULL CHECK (
        groundloop_m5_sorted_unique_text_array(refuting_observation_ids)
    ),
    complete_group_count integer NOT NULL CHECK (complete_group_count >= 0),
    complete_group_ids text[] NOT NULL CHECK (
        groundloop_m5_sorted_unique_text_array(complete_group_ids)
        AND complete_group_count = cardinality(complete_group_ids)
    ),
    status text NOT NULL CHECK (
        status IN ('supported', 'unsupported', 'refuted', 'conflicted')
    ),
    decision_policy_version text NOT NULL
        REFERENCES groundloop_decision_policy(policy_version),
    certificate_digest char(64) NOT NULL CHECK (
        certificate_digest ~ '^[0-9a-f]{64}$'
    ),
    updated_revision bigint NOT NULL CHECK (updated_revision >= 0),
    PRIMARY KEY (epoch_id, claim_id)
);

CREATE TABLE groundloop_m5_working_answer_state (
    epoch_id bigint NOT NULL REFERENCES groundloop_m5_update(epoch_id),
    answer_version_id text NOT NULL
        REFERENCES groundloop_answer_version(answer_version_id),
    required_claim_count integer NOT NULL CHECK (required_claim_count > 0),
    supported_count integer NOT NULL CHECK (supported_count >= 0),
    unsupported_count integer NOT NULL CHECK (unsupported_count >= 0),
    refuted_count integer NOT NULL CHECK (refuted_count >= 0),
    conflicted_count integer NOT NULL CHECK (conflicted_count >= 0),
    status text NOT NULL CHECK (
        status IN (
            'valid', 'partially_supported', 'unsupported',
            'conflicted', 'contradicted'
        )
    ),
    updated_revision bigint NOT NULL CHECK (updated_revision >= 0),
    PRIMARY KEY (epoch_id, answer_version_id),
    CHECK (
        supported_count + unsupported_count + refuted_count + conflicted_count
        = required_claim_count
    )
);

CREATE FUNCTION groundloop_m5_validate_working_state_mutation()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    current_revision bigint;
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'M5 working state cannot be deleted';
    END IF;
    PERFORM groundloop_m5_assert_writable_epoch(NEW.epoch_id);
    SELECT revision
    INTO STRICT current_revision
    FROM groundloop_epoch
    WHERE epoch_id = NEW.epoch_id;
    IF NEW.updated_revision <> current_revision THEN
        RAISE EXCEPTION 'M5 working state must name the current epoch revision';
    END IF;
    IF TG_OP = 'UPDATE' THEN
        IF NEW.epoch_id <> OLD.epoch_id THEN
            RAISE EXCEPTION 'M5 working-state epoch identity cannot change';
        END IF;
        IF NEW.updated_revision <= OLD.updated_revision THEN
            RAISE EXCEPTION 'M5 working-state revision must increase';
        END IF;
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER groundloop_m5_working_requirement_state_guard
BEFORE INSERT OR UPDATE OR DELETE ON groundloop_m5_working_requirement_state
FOR EACH ROW EXECUTE FUNCTION groundloop_m5_validate_working_state_mutation();
CREATE TRIGGER groundloop_m5_working_group_state_guard
BEFORE INSERT OR UPDATE OR DELETE ON groundloop_m5_working_group_state
FOR EACH ROW EXECUTE FUNCTION groundloop_m5_validate_working_state_mutation();
CREATE TRIGGER groundloop_m5_working_claim_state_guard
BEFORE INSERT OR UPDATE OR DELETE ON groundloop_m5_working_claim_state
FOR EACH ROW EXECUTE FUNCTION groundloop_m5_validate_working_state_mutation();
CREATE TRIGGER groundloop_m5_working_answer_state_guard
BEFORE INSERT OR UPDATE OR DELETE ON groundloop_m5_working_answer_state
FOR EACH ROW EXECUTE FUNCTION groundloop_m5_validate_working_state_mutation();

-- Current materialized projections are separate from the sealed interval
-- history and contain no Hall-mask implementation state.
CREATE TABLE groundloop_m5_requirement_state_materialized (
    requirement_version_id text PRIMARY KEY
        REFERENCES groundloop_m5_requirement_version(requirement_version_id),
    witness_hashes text[] NOT NULL CHECK (
        groundloop_m5_sorted_unique_sha256_array(witness_hashes)
    ),
    supporting_observation_ids text[] NOT NULL CHECK (
        groundloop_m5_sorted_unique_text_array(supporting_observation_ids)
    ),
    witness_count integer NOT NULL CHECK (
        witness_count >= 0 AND witness_count = cardinality(witness_hashes)
    ),
    satisfied boolean NOT NULL CHECK (satisfied = (witness_count > 0)),
    decision_policy_version text NOT NULL
        REFERENCES groundloop_decision_policy(policy_version),
    updated_epoch bigint NOT NULL REFERENCES groundloop_epoch(epoch_id),
    updated_revision bigint NOT NULL CHECK (updated_revision >= 0)
);

CREATE TABLE groundloop_m5_group_state_materialized (
    group_version_id text PRIMARY KEY
        REFERENCES groundloop_m5_group_version(group_version_id),
    requirement_count integer NOT NULL CHECK (requirement_count BETWEEN 1 AND 8),
    satisfied_count integer NOT NULL CHECK (
        satisfied_count BETWEEN 0 AND requirement_count
    ),
    matching_size integer NOT NULL CHECK (
        matching_size BETWEEN 0 AND requirement_count
    ),
    complete boolean NOT NULL CHECK (complete = (matching_size = requirement_count)),
    decision_policy_version text NOT NULL
        REFERENCES groundloop_decision_policy(policy_version),
    certificate_digest char(64),
    updated_epoch bigint NOT NULL REFERENCES groundloop_epoch(epoch_id),
    updated_revision bigint NOT NULL CHECK (updated_revision >= 0),
    CHECK (
        (complete AND certificate_digest ~ '^[0-9a-f]{64}$')
        OR (NOT complete AND certificate_digest IS NULL)
    )
);

CREATE TABLE groundloop_m5_claim_state_materialized (
    claim_id text PRIMARY KEY REFERENCES groundloop_claim(claim_id),
    support_count integer NOT NULL CHECK (support_count >= 0),
    refute_count integer NOT NULL CHECK (refute_count >= 0),
    best_support_score double precision,
    best_refute_score double precision,
    supporting_observation_ids text[] NOT NULL CHECK (
        groundloop_m5_sorted_unique_text_array(supporting_observation_ids)
    ),
    refuting_observation_ids text[] NOT NULL CHECK (
        groundloop_m5_sorted_unique_text_array(refuting_observation_ids)
    ),
    complete_group_count integer NOT NULL CHECK (complete_group_count >= 0),
    complete_group_ids text[] NOT NULL CHECK (
        groundloop_m5_sorted_unique_text_array(complete_group_ids)
        AND complete_group_count = cardinality(complete_group_ids)
    ),
    status text NOT NULL CHECK (
        status IN ('supported', 'unsupported', 'refuted', 'conflicted')
    ),
    decision_policy_version text NOT NULL
        REFERENCES groundloop_decision_policy(policy_version),
    certificate_digest char(64) NOT NULL CHECK (
        certificate_digest ~ '^[0-9a-f]{64}$'
    ),
    updated_epoch bigint NOT NULL REFERENCES groundloop_epoch(epoch_id),
    updated_revision bigint NOT NULL CHECK (updated_revision >= 0)
);

CREATE TABLE groundloop_m5_answer_state_materialized (
    answer_version_id text PRIMARY KEY
        REFERENCES groundloop_answer_version(answer_version_id),
    required_claim_count integer NOT NULL CHECK (required_claim_count > 0),
    supported_count integer NOT NULL CHECK (supported_count >= 0),
    unsupported_count integer NOT NULL CHECK (unsupported_count >= 0),
    refuted_count integer NOT NULL CHECK (refuted_count >= 0),
    conflicted_count integer NOT NULL CHECK (conflicted_count >= 0),
    status text NOT NULL CHECK (
        status IN (
            'valid', 'partially_supported', 'unsupported',
            'conflicted', 'contradicted'
        )
    ),
    updated_epoch bigint NOT NULL REFERENCES groundloop_epoch(epoch_id),
    updated_revision bigint NOT NULL CHECK (updated_revision >= 0),
    CHECK (
        supported_count + unsupported_count + refuted_count + conflicted_count
        = required_claim_count
    )
);

CREATE TABLE groundloop_m5_published_requirement_state (
    requirement_version_id text NOT NULL
        REFERENCES groundloop_m5_requirement_version(requirement_version_id),
    valid_from_epoch bigint NOT NULL REFERENCES groundloop_epoch(epoch_id),
    valid_to_epoch bigint REFERENCES groundloop_epoch(epoch_id),
    sealed_revision bigint NOT NULL CHECK (sealed_revision >= 0),
    witness_hashes text[] NOT NULL CHECK (
        groundloop_m5_sorted_unique_sha256_array(witness_hashes)
    ),
    supporting_observation_ids text[] NOT NULL CHECK (
        groundloop_m5_sorted_unique_text_array(supporting_observation_ids)
    ),
    witness_count integer NOT NULL CHECK (
        witness_count >= 0 AND witness_count = cardinality(witness_hashes)
    ),
    satisfied boolean NOT NULL CHECK (satisfied = (witness_count > 0)),
    decision_policy_version text NOT NULL
        REFERENCES groundloop_decision_policy(policy_version),
    PRIMARY KEY (requirement_version_id, valid_from_epoch),
    CHECK (valid_to_epoch IS NULL OR valid_to_epoch > valid_from_epoch)
);

CREATE TABLE groundloop_m5_published_group_state (
    group_version_id text NOT NULL
        REFERENCES groundloop_m5_group_version(group_version_id),
    valid_from_epoch bigint NOT NULL REFERENCES groundloop_epoch(epoch_id),
    valid_to_epoch bigint REFERENCES groundloop_epoch(epoch_id),
    sealed_revision bigint NOT NULL CHECK (sealed_revision >= 0),
    requirement_count integer NOT NULL CHECK (requirement_count BETWEEN 1 AND 8),
    satisfied_count integer NOT NULL CHECK (
        satisfied_count BETWEEN 0 AND requirement_count
    ),
    matching_size integer NOT NULL CHECK (
        matching_size BETWEEN 0 AND requirement_count
    ),
    complete boolean NOT NULL CHECK (complete = (matching_size = requirement_count)),
    decision_policy_version text NOT NULL
        REFERENCES groundloop_decision_policy(policy_version),
    certificate_digest char(64),
    PRIMARY KEY (group_version_id, valid_from_epoch),
    CHECK (valid_to_epoch IS NULL OR valid_to_epoch > valid_from_epoch),
    CHECK (
        (complete AND certificate_digest ~ '^[0-9a-f]{64}$')
        OR (NOT complete AND certificate_digest IS NULL)
    )
);

CREATE TABLE groundloop_m5_published_claim_state (
    claim_id text NOT NULL REFERENCES groundloop_claim(claim_id),
    valid_from_epoch bigint NOT NULL REFERENCES groundloop_epoch(epoch_id),
    valid_to_epoch bigint REFERENCES groundloop_epoch(epoch_id),
    sealed_revision bigint NOT NULL CHECK (sealed_revision >= 0),
    support_count integer NOT NULL CHECK (support_count >= 0),
    refute_count integer NOT NULL CHECK (refute_count >= 0),
    best_support_score double precision,
    best_refute_score double precision,
    supporting_observation_ids text[] NOT NULL CHECK (
        groundloop_m5_sorted_unique_text_array(supporting_observation_ids)
    ),
    refuting_observation_ids text[] NOT NULL CHECK (
        groundloop_m5_sorted_unique_text_array(refuting_observation_ids)
    ),
    complete_group_count integer NOT NULL CHECK (complete_group_count >= 0),
    complete_group_ids text[] NOT NULL CHECK (
        groundloop_m5_sorted_unique_text_array(complete_group_ids)
        AND complete_group_count = cardinality(complete_group_ids)
    ),
    status text NOT NULL CHECK (
        status IN ('supported', 'unsupported', 'refuted', 'conflicted')
    ),
    decision_policy_version text NOT NULL
        REFERENCES groundloop_decision_policy(policy_version),
    certificate_digest char(64) NOT NULL CHECK (
        certificate_digest ~ '^[0-9a-f]{64}$'
    ),
    PRIMARY KEY (claim_id, valid_from_epoch),
    CHECK (valid_to_epoch IS NULL OR valid_to_epoch > valid_from_epoch)
);

CREATE TABLE groundloop_m5_published_answer_state (
    answer_version_id text NOT NULL
        REFERENCES groundloop_answer_version(answer_version_id),
    valid_from_epoch bigint NOT NULL REFERENCES groundloop_epoch(epoch_id),
    valid_to_epoch bigint REFERENCES groundloop_epoch(epoch_id),
    sealed_revision bigint NOT NULL CHECK (sealed_revision >= 0),
    required_claim_count integer NOT NULL CHECK (required_claim_count > 0),
    supported_count integer NOT NULL CHECK (supported_count >= 0),
    unsupported_count integer NOT NULL CHECK (unsupported_count >= 0),
    refuted_count integer NOT NULL CHECK (refuted_count >= 0),
    conflicted_count integer NOT NULL CHECK (conflicted_count >= 0),
    status text NOT NULL CHECK (
        status IN (
            'valid', 'partially_supported', 'unsupported',
            'conflicted', 'contradicted'
        )
    ),
    PRIMARY KEY (answer_version_id, valid_from_epoch),
    CHECK (valid_to_epoch IS NULL OR valid_to_epoch > valid_from_epoch),
    CHECK (
        supported_count + unsupported_count + refuted_count + conflicted_count
        = required_claim_count
    )
);

ALTER TABLE groundloop_m5_published_requirement_state
    ADD CONSTRAINT groundloop_m5_published_requirement_state_no_overlap
    EXCLUDE USING gist (
        requirement_version_id WITH =,
        int8range(valid_from_epoch, valid_to_epoch, '[)') WITH &&
    );
ALTER TABLE groundloop_m5_published_group_state
    ADD CONSTRAINT groundloop_m5_published_group_state_no_overlap
    EXCLUDE USING gist (
        group_version_id WITH =,
        int8range(valid_from_epoch, valid_to_epoch, '[)') WITH &&
    );
ALTER TABLE groundloop_m5_published_claim_state
    ADD CONSTRAINT groundloop_m5_published_claim_state_no_overlap
    EXCLUDE USING gist (
        claim_id WITH =,
        int8range(valid_from_epoch, valid_to_epoch, '[)') WITH &&
    );
ALTER TABLE groundloop_m5_published_answer_state
    ADD CONSTRAINT groundloop_m5_published_answer_state_no_overlap
    EXCLUDE USING gist (
        answer_version_id WITH =,
        int8range(valid_from_epoch, valid_to_epoch, '[)') WITH &&
    );

CREATE UNIQUE INDEX groundloop_m5_one_current_published_requirement_state
    ON groundloop_m5_published_requirement_state(requirement_version_id)
    WHERE valid_to_epoch IS NULL;
CREATE UNIQUE INDEX groundloop_m5_one_current_published_group_state
    ON groundloop_m5_published_group_state(group_version_id)
    WHERE valid_to_epoch IS NULL;
CREATE UNIQUE INDEX groundloop_m5_one_current_published_claim_state
    ON groundloop_m5_published_claim_state(claim_id)
    WHERE valid_to_epoch IS NULL;
CREATE UNIQUE INDEX groundloop_m5_one_current_published_answer_state
    ON groundloop_m5_published_answer_state(answer_version_id)
    WHERE valid_to_epoch IS NULL;

CREATE FUNCTION groundloop_m5_validate_published_state_change()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'published M5 state cannot be deleted';
    END IF;
    IF (to_jsonb(NEW) - 'valid_to_epoch')
       IS DISTINCT FROM (to_jsonb(OLD) - 'valid_to_epoch')
       OR OLD.valid_to_epoch IS NOT NULL
       OR NEW.valid_to_epoch IS NULL
       OR NEW.valid_to_epoch <= OLD.valid_from_epoch THEN
        RAISE EXCEPTION 'published M5 state may close exactly once';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER groundloop_m5_published_requirement_state_close_once
BEFORE UPDATE OR DELETE ON groundloop_m5_published_requirement_state
FOR EACH ROW EXECUTE FUNCTION groundloop_m5_validate_published_state_change();
CREATE TRIGGER groundloop_m5_published_group_state_close_once
BEFORE UPDATE OR DELETE ON groundloop_m5_published_group_state
FOR EACH ROW EXECUTE FUNCTION groundloop_m5_validate_published_state_change();
CREATE TRIGGER groundloop_m5_published_claim_state_close_once
BEFORE UPDATE OR DELETE ON groundloop_m5_published_claim_state
FOR EACH ROW EXECUTE FUNCTION groundloop_m5_validate_published_state_change();
CREATE TRIGGER groundloop_m5_published_answer_state_close_once
BEFORE UPDATE OR DELETE ON groundloop_m5_published_answer_state
FOR EACH ROW EXECUTE FUNCTION groundloop_m5_validate_published_state_change();

-- Byte-total digest helper used by deferred artifact and group-record checks.
CREATE FUNCTION groundloop_m5_digest_text_fields(fields_to_hash text[])
RETURNS char(64)
LANGUAGE plpgsql
IMMUTABLE
STRICT
PARALLEL SAFE
AS $$
DECLARE
    field text;
    encoded bytea;
    payload bytea := ''::bytea;
BEGIN
    FOREACH field IN ARRAY fields_to_hash LOOP
        IF field IS NULL THEN
            RAISE EXCEPTION 'M5 digest fields cannot contain SQL NULL';
        END IF;
        encoded := convert_to(field, 'UTF8');
        payload := payload || int8send(octet_length(encoded)::bigint) || encoded;
    END LOOP;
    RETURN encode(digest(payload, 'sha256'), 'hex');
END;
$$;

CREATE FUNCTION groundloop_m5_expected_semantic_structure(
    group_version_id_to_hash text
)
RETURNS char(64)
LANGUAGE plpgsql
STABLE
STRICT
AS $$
DECLARE
    group_kind text;
    requirement_count integer;
    requirement_hash text;
    fields text[];
BEGIN
    SELECT group_type
    INTO STRICT group_kind
    FROM groundloop_m5_group_version
    WHERE group_version_id = group_version_id_to_hash;
    SELECT count(*)::integer
    INTO requirement_count
    FROM groundloop_m5_requirement_version
    WHERE group_version_id = group_version_id_to_hash;
    fields := ARRAY[
        'm5-semantic-structure-v1', 'enum', group_kind,
        'sequence', 'int', requirement_count::text
    ];
    FOR requirement_hash IN
        SELECT requirement_text_hash::text
        FROM groundloop_m5_requirement_version
        WHERE group_version_id = group_version_id_to_hash
        ORDER BY requirement_text_hash
    LOOP
        fields := fields || ARRAY['sha256', requirement_hash];
    END LOOP;
    RETURN groundloop_m5_digest_text_fields(fields);
END;
$$;

CREATE FUNCTION groundloop_m5_expected_group_record(
    group_version_id_to_hash text
)
RETURNS char(64)
LANGUAGE plpgsql
STABLE
STRICT
AS $$
DECLARE
    group_row record;
    requirement_row record;
    requirement_count integer;
    fields text[];
BEGIN
    SELECT version.*, family.claim_id
    INTO STRICT group_row
    FROM groundloop_m5_group_version AS version
    JOIN groundloop_m5_group_family AS family
      ON family.group_family_id = version.group_family_id
    WHERE version.group_version_id = group_version_id_to_hash;
    SELECT count(*)::integer
    INTO requirement_count
    FROM groundloop_m5_requirement_version
    WHERE group_version_id = group_version_id_to_hash;

    fields := ARRAY[
        'm5-group-record-v1',
        'text', group_row.claim_id,
        'text', group_row.group_version_id,
        'text', group_row.group_family_id,
        'enum', group_row.group_type,
        'enum', group_row.construction_kind,
        'text', group_row.construction_source_id
    ];
    fields := fields || CASE WHEN group_row.constructor_model_id IS NULL
        THEN ARRAY['null']
        ELSE ARRAY['text', group_row.constructor_model_id]
    END;
    fields := fields || CASE WHEN group_row.constructor_model_version IS NULL
        THEN ARRAY['null']
        ELSE ARRAY['text', group_row.constructor_model_version]
    END;
    fields := fields || CASE WHEN group_row.constructor_prompt_version IS NULL
        THEN ARRAY['null']
        ELSE ARRAY['text', group_row.constructor_prompt_version]
    END;
    fields := fields || CASE WHEN group_row.supersedes_group_version_id IS NULL
        THEN ARRAY['null']
        ELSE ARRAY['text', group_row.supersedes_group_version_id]
    END;
    fields := fields || ARRAY[
        'sha256', group_row.semantic_structure_hash::text,
        'sequence', 'int', requirement_count::text
    ];
    FOR requirement_row IN
        SELECT *
        FROM groundloop_m5_requirement_version
        WHERE group_version_id = group_version_id_to_hash
        ORDER BY ordinal
    LOOP
        fields := fields || ARRAY[
            'sequence', 'int', '8',
            'text', requirement_row.requirement_version_id,
            'int', requirement_row.ordinal::text,
            'text', requirement_row.requirement_text,
            'sha256', requirement_row.requirement_text_hash::text
        ];
        fields := fields || CASE WHEN requirement_row.constructor_model_id IS NULL
            THEN ARRAY['null']
            ELSE ARRAY['text', requirement_row.constructor_model_id]
        END;
        fields := fields || CASE
            WHEN requirement_row.constructor_model_version IS NULL
            THEN ARRAY['null']
            ELSE ARRAY['text', requirement_row.constructor_model_version]
        END;
        fields := fields || CASE
            WHEN requirement_row.constructor_prompt_version IS NULL
            THEN ARRAY['null']
            ELSE ARRAY['text', requirement_row.constructor_prompt_version]
        END;
        fields := fields || CASE
            WHEN requirement_row.supersedes_requirement_version_id IS NULL
            THEN ARRAY['null']
            ELSE ARRAY['text', requirement_row.supersedes_requirement_version_id]
        END;
    END LOOP;
    RETURN groundloop_m5_digest_text_fields(fields);
END;
$$;

CREATE TABLE groundloop_m5_group_certificate_artifact (
    certificate_digest char(64) PRIMARY KEY CHECK (
        certificate_digest ~ '^[0-9a-f]{64}$'
    ),
    decision_policy_version text NOT NULL
        REFERENCES groundloop_decision_policy(policy_version),
    certificate_version text NOT NULL CHECK (
        certificate_version = 'm5-group-certificate-v1'
    ),
    group_version_id text NOT NULL
        REFERENCES groundloop_m5_group_version(group_version_id),
    requirement_count integer NOT NULL CHECK (requirement_count BETWEEN 1 AND 8)
);

CREATE TABLE groundloop_m5_group_certificate_artifact_row (
    certificate_digest char(64) NOT NULL
        REFERENCES groundloop_m5_group_certificate_artifact(certificate_digest)
        DEFERRABLE INITIALLY DEFERRED,
    requirement_ordinal integer NOT NULL CHECK (requirement_ordinal BETWEEN 0 AND 7),
    requirement_version_id text NOT NULL
        REFERENCES groundloop_m5_requirement_version(requirement_version_id),
    text_hash char(64) NOT NULL CHECK (text_hash ~ '^[0-9a-f]{64}$'),
    selected_observation_id text NOT NULL
        REFERENCES groundloop_semantic_observation(observation_id),
    PRIMARY KEY (certificate_digest, requirement_ordinal),
    UNIQUE (certificate_digest, requirement_version_id),
    UNIQUE (certificate_digest, text_hash)
);

CREATE TABLE groundloop_m5_working_group_certificate_binding (
    epoch_id bigint NOT NULL REFERENCES groundloop_m5_update(epoch_id),
    group_version_id text NOT NULL
        REFERENCES groundloop_m5_group_version(group_version_id),
    valid_from_revision bigint NOT NULL CHECK (valid_from_revision >= 0),
    valid_to_revision bigint,
    certificate_digest char(64) NOT NULL
        REFERENCES groundloop_m5_group_certificate_artifact(certificate_digest),
    PRIMARY KEY (epoch_id, group_version_id, valid_from_revision),
    CHECK (
        valid_to_revision IS NULL
        OR valid_to_revision > valid_from_revision
    )
);

ALTER TABLE groundloop_m5_working_group_certificate_binding
    ADD CONSTRAINT groundloop_m5_working_group_binding_no_overlap
    EXCLUDE USING gist (
        epoch_id WITH =,
        group_version_id WITH =,
        int8range(valid_from_revision, valid_to_revision, '[)') WITH &&
    ) DEFERRABLE INITIALLY DEFERRED;

CREATE UNIQUE INDEX groundloop_m5_one_open_working_group_binding
    ON groundloop_m5_working_group_certificate_binding(epoch_id, group_version_id)
    WHERE valid_to_revision IS NULL;

CREATE TABLE groundloop_m5_claim_certificate_artifact (
    certificate_digest char(64) PRIMARY KEY CHECK (
        certificate_digest ~ '^[0-9a-f]{64}$'
    ),
    certificate_version text NOT NULL CHECK (
        certificate_version = 'm5-claim-certificate-v2'
    ),
    claim_id text NOT NULL REFERENCES groundloop_claim(claim_id),
    decision_policy_version text NOT NULL
        REFERENCES groundloop_decision_policy(policy_version),
    support_kind text NOT NULL CHECK (support_kind IN ('none', 'direct', 'group')),
    direct_support_observation_id text
        REFERENCES groundloop_semantic_observation(observation_id),
    group_version_id text REFERENCES groundloop_m5_group_version(group_version_id),
    group_certificate_digest char(64)
        REFERENCES groundloop_m5_group_certificate_artifact(certificate_digest),
    direct_refute_observation_id text
        REFERENCES groundloop_semantic_observation(observation_id),
    CHECK (
        (support_kind = 'none'
         AND direct_support_observation_id IS NULL
         AND group_version_id IS NULL
         AND group_certificate_digest IS NULL)
        OR
        (support_kind = 'direct'
         AND direct_support_observation_id IS NOT NULL
         AND group_version_id IS NULL
         AND group_certificate_digest IS NULL)
        OR
        (support_kind = 'group'
         AND direct_support_observation_id IS NULL
         AND group_version_id IS NOT NULL
         AND group_certificate_digest IS NOT NULL)
    )
);

CREATE TABLE groundloop_m5_working_claim_certificate_binding (
    epoch_id bigint NOT NULL REFERENCES groundloop_m5_update(epoch_id),
    claim_id text NOT NULL REFERENCES groundloop_claim(claim_id),
    valid_from_revision bigint NOT NULL CHECK (valid_from_revision >= 0),
    valid_to_revision bigint,
    certificate_digest char(64) NOT NULL
        REFERENCES groundloop_m5_claim_certificate_artifact(certificate_digest),
    PRIMARY KEY (epoch_id, claim_id, valid_from_revision),
    CHECK (
        valid_to_revision IS NULL
        OR valid_to_revision > valid_from_revision
    )
);

ALTER TABLE groundloop_m5_working_claim_certificate_binding
    ADD CONSTRAINT groundloop_m5_working_claim_binding_no_overlap
    EXCLUDE USING gist (
        epoch_id WITH =,
        claim_id WITH =,
        int8range(valid_from_revision, valid_to_revision, '[)') WITH &&
    ) DEFERRABLE INITIALLY DEFERRED;

CREATE UNIQUE INDEX groundloop_m5_one_open_working_claim_binding
    ON groundloop_m5_working_claim_certificate_binding(epoch_id, claim_id)
    WHERE valid_to_revision IS NULL;

CREATE TABLE groundloop_m5_published_group_certificate_binding (
    group_version_id text NOT NULL
        REFERENCES groundloop_m5_group_version(group_version_id),
    valid_from_epoch bigint NOT NULL REFERENCES groundloop_epoch(epoch_id),
    valid_to_epoch bigint REFERENCES groundloop_epoch(epoch_id),
    sealed_revision bigint NOT NULL CHECK (sealed_revision >= 0),
    certificate_digest char(64) NOT NULL
        REFERENCES groundloop_m5_group_certificate_artifact(certificate_digest),
    PRIMARY KEY (group_version_id, valid_from_epoch),
    CHECK (valid_to_epoch IS NULL OR valid_to_epoch > valid_from_epoch)
);

CREATE TABLE groundloop_m5_published_claim_certificate_binding (
    claim_id text NOT NULL REFERENCES groundloop_claim(claim_id),
    valid_from_epoch bigint NOT NULL REFERENCES groundloop_epoch(epoch_id),
    valid_to_epoch bigint REFERENCES groundloop_epoch(epoch_id),
    sealed_revision bigint NOT NULL CHECK (sealed_revision >= 0),
    certificate_digest char(64) NOT NULL
        REFERENCES groundloop_m5_claim_certificate_artifact(certificate_digest),
    PRIMARY KEY (claim_id, valid_from_epoch),
    CHECK (valid_to_epoch IS NULL OR valid_to_epoch > valid_from_epoch)
);

ALTER TABLE groundloop_m5_published_group_certificate_binding
    ADD CONSTRAINT groundloop_m5_published_group_binding_no_overlap
    EXCLUDE USING gist (
        group_version_id WITH =,
        int8range(valid_from_epoch, valid_to_epoch, '[)') WITH &&
    );
ALTER TABLE groundloop_m5_published_claim_certificate_binding
    ADD CONSTRAINT groundloop_m5_published_claim_binding_no_overlap
    EXCLUDE USING gist (
        claim_id WITH =,
        int8range(valid_from_epoch, valid_to_epoch, '[)') WITH &&
    );

CREATE UNIQUE INDEX groundloop_m5_one_current_published_group_binding
    ON groundloop_m5_published_group_certificate_binding(group_version_id)
    WHERE valid_to_epoch IS NULL;
CREATE UNIQUE INDEX groundloop_m5_one_current_published_claim_binding
    ON groundloop_m5_published_claim_certificate_binding(claim_id)
    WHERE valid_to_epoch IS NULL;

CREATE TRIGGER groundloop_m5_group_certificate_artifact_immutable
BEFORE UPDATE OR DELETE ON groundloop_m5_group_certificate_artifact
FOR EACH ROW EXECUTE FUNCTION groundloop_m5_reject_immutable_row();
CREATE TRIGGER groundloop_m5_group_certificate_row_immutable
BEFORE UPDATE OR DELETE ON groundloop_m5_group_certificate_artifact_row
FOR EACH ROW EXECUTE FUNCTION groundloop_m5_reject_immutable_row();
CREATE TRIGGER groundloop_m5_claim_certificate_artifact_immutable
BEFORE UPDATE OR DELETE ON groundloop_m5_claim_certificate_artifact
FOR EACH ROW EXECUTE FUNCTION groundloop_m5_reject_immutable_row();
CREATE TRIGGER groundloop_m5_working_group_binding_close_once
BEFORE INSERT OR UPDATE OR DELETE ON groundloop_m5_working_group_certificate_binding
FOR EACH ROW EXECUTE FUNCTION groundloop_m5_validate_revision_interval_mutation();
CREATE TRIGGER groundloop_m5_working_claim_binding_close_once
BEFORE INSERT OR UPDATE OR DELETE ON groundloop_m5_working_claim_certificate_binding
FOR EACH ROW EXECUTE FUNCTION groundloop_m5_validate_revision_interval_mutation();
CREATE TRIGGER groundloop_m5_published_group_binding_close_once
BEFORE UPDATE OR DELETE ON groundloop_m5_published_group_certificate_binding
FOR EACH ROW EXECUTE FUNCTION groundloop_m5_validate_published_state_change();
CREATE TRIGGER groundloop_m5_published_claim_binding_close_once
BEFORE UPDATE OR DELETE ON groundloop_m5_published_claim_certificate_binding
FOR EACH ROW EXECUTE FUNCTION groundloop_m5_validate_published_state_change();

ALTER TABLE groundloop_m5_working_group_state
    ADD CONSTRAINT groundloop_m5_working_group_state_certificate_fkey
    FOREIGN KEY (certificate_digest)
    REFERENCES groundloop_m5_group_certificate_artifact(certificate_digest)
    DEFERRABLE INITIALLY DEFERRED;
ALTER TABLE groundloop_m5_group_state_materialized
    ADD CONSTRAINT groundloop_m5_group_state_materialized_certificate_fkey
    FOREIGN KEY (certificate_digest)
    REFERENCES groundloop_m5_group_certificate_artifact(certificate_digest)
    DEFERRABLE INITIALLY DEFERRED;
ALTER TABLE groundloop_m5_published_group_state
    ADD CONSTRAINT groundloop_m5_published_group_state_certificate_fkey
    FOREIGN KEY (certificate_digest)
    REFERENCES groundloop_m5_group_certificate_artifact(certificate_digest)
    DEFERRABLE INITIALLY DEFERRED;
ALTER TABLE groundloop_m5_working_claim_state
    ADD CONSTRAINT groundloop_m5_working_claim_state_certificate_fkey
    FOREIGN KEY (certificate_digest)
    REFERENCES groundloop_m5_claim_certificate_artifact(certificate_digest)
    DEFERRABLE INITIALLY DEFERRED;
ALTER TABLE groundloop_m5_claim_state_materialized
    ADD CONSTRAINT groundloop_m5_claim_state_materialized_certificate_fkey
    FOREIGN KEY (certificate_digest)
    REFERENCES groundloop_m5_claim_certificate_artifact(certificate_digest)
    DEFERRABLE INITIALLY DEFERRED;
ALTER TABLE groundloop_m5_published_claim_state
    ADD CONSTRAINT groundloop_m5_published_claim_state_certificate_fkey
    FOREIGN KEY (certificate_digest)
    REFERENCES groundloop_m5_claim_certificate_artifact(certificate_digest)
    DEFERRABLE INITIALLY DEFERRED;

-- Working bindings are exact revision history, while published state and
-- binding intervals must advance together.  The as-of validators are supplied
-- by the second member of the atomic M5 schema bundle.
CREATE FUNCTION groundloop_m5_assert_certificate_bindings()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM groundloop_m5_working_group_certificate_binding AS binding
        WHERE NOT groundloop_m5_group_certificate_valid_at(
            binding.certificate_digest,
            binding.epoch_id,
            binding.valid_from_revision
        )
    ) THEN
        RAISE EXCEPTION 'invalid M5 working group certificate binding';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM groundloop_m5_working_claim_certificate_binding AS binding
        WHERE NOT groundloop_m5_claim_certificate_valid_at(
            binding.certificate_digest,
            binding.epoch_id,
            binding.valid_from_revision
        )
    ) THEN
        RAISE EXCEPTION 'invalid M5 working claim certificate binding';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM groundloop_m5_working_group_state AS state
        WHERE (
            state.complete
            AND NOT EXISTS (
                SELECT 1
                FROM groundloop_m5_working_group_certificate_binding AS binding
                WHERE binding.epoch_id = state.epoch_id
                  AND binding.group_version_id = state.group_version_id
                  AND binding.certificate_digest = state.certificate_digest
                  AND binding.valid_from_revision <= state.updated_revision
                  AND (
                      binding.valid_to_revision IS NULL
                      OR state.updated_revision < binding.valid_to_revision
                  )
            )
        ) OR (
            NOT state.complete
            AND EXISTS (
                SELECT 1
                FROM groundloop_m5_working_group_certificate_binding AS binding
                WHERE binding.epoch_id = state.epoch_id
                  AND binding.group_version_id = state.group_version_id
                  AND binding.valid_from_revision <= state.updated_revision
                  AND (
                      binding.valid_to_revision IS NULL
                      OR state.updated_revision < binding.valid_to_revision
                  )
            )
        )
    ) THEN
        RAISE EXCEPTION 'M5 working group state and certificate binding differ';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM groundloop_m5_working_claim_state AS state
        WHERE NOT EXISTS (
            SELECT 1
            FROM groundloop_m5_working_claim_certificate_binding AS binding
            WHERE binding.epoch_id = state.epoch_id
              AND binding.claim_id = state.claim_id
              AND binding.certificate_digest = state.certificate_digest
              AND binding.valid_from_revision <= state.updated_revision
              AND (
                  binding.valid_to_revision IS NULL
                  OR state.updated_revision < binding.valid_to_revision
              )
        )
    ) THEN
        RAISE EXCEPTION 'M5 working claim state and certificate binding differ';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM groundloop_m5_published_group_state AS state
        WHERE state.complete
          AND NOT EXISTS (
              SELECT 1
              FROM groundloop_m5_published_group_certificate_binding AS binding
              WHERE binding.group_version_id = state.group_version_id
                AND binding.valid_from_epoch = state.valid_from_epoch
                AND binding.valid_to_epoch IS NOT DISTINCT FROM state.valid_to_epoch
                AND binding.sealed_revision = state.sealed_revision
                AND binding.certificate_digest = state.certificate_digest
          )
    ) THEN
        RAISE EXCEPTION 'published M5 group state and binding differ';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM groundloop_m5_published_claim_state AS state
        WHERE NOT EXISTS (
            SELECT 1
            FROM groundloop_m5_published_claim_certificate_binding AS binding
            WHERE binding.claim_id = state.claim_id
              AND binding.valid_from_epoch = state.valid_from_epoch
              AND binding.valid_to_epoch IS NOT DISTINCT FROM state.valid_to_epoch
              AND binding.sealed_revision = state.sealed_revision
              AND binding.certificate_digest = state.certificate_digest
        )
    ) THEN
        RAISE EXCEPTION 'published M5 claim state and binding differ';
    END IF;
    RETURN NULL;
END;
$$;

CREATE CONSTRAINT TRIGGER groundloop_m5_working_group_binding_integrity
AFTER INSERT OR UPDATE OR DELETE ON groundloop_m5_working_group_certificate_binding
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION groundloop_m5_assert_certificate_bindings();
CREATE CONSTRAINT TRIGGER groundloop_m5_working_claim_binding_integrity
AFTER INSERT OR UPDATE OR DELETE ON groundloop_m5_working_claim_certificate_binding
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION groundloop_m5_assert_certificate_bindings();
CREATE CONSTRAINT TRIGGER groundloop_m5_working_group_state_binding_integrity
AFTER INSERT OR UPDATE OR DELETE ON groundloop_m5_working_group_state
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION groundloop_m5_assert_certificate_bindings();
CREATE CONSTRAINT TRIGGER groundloop_m5_working_claim_state_binding_integrity
AFTER INSERT OR UPDATE OR DELETE ON groundloop_m5_working_claim_state
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION groundloop_m5_assert_certificate_bindings();
CREATE CONSTRAINT TRIGGER groundloop_m5_published_group_binding_integrity
AFTER INSERT OR UPDATE OR DELETE ON groundloop_m5_published_group_certificate_binding
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION groundloop_m5_assert_certificate_bindings();
CREATE CONSTRAINT TRIGGER groundloop_m5_published_claim_binding_integrity
AFTER INSERT OR UPDATE OR DELETE ON groundloop_m5_published_claim_certificate_binding
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION groundloop_m5_assert_certificate_bindings();
CREATE CONSTRAINT TRIGGER groundloop_m5_published_group_state_binding_integrity
AFTER INSERT OR UPDATE OR DELETE ON groundloop_m5_published_group_state
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION groundloop_m5_assert_certificate_bindings();
CREATE CONSTRAINT TRIGGER groundloop_m5_published_claim_state_binding_integrity
AFTER INSERT OR UPDATE OR DELETE ON groundloop_m5_published_claim_state
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION groundloop_m5_assert_certificate_bindings();

CREATE FUNCTION groundloop_m5_expected_group_certificate(
    certificate_digest_to_check char(64)
)
RETURNS char(64)
LANGUAGE plpgsql
STABLE
STRICT
AS $$
DECLARE
    artifact record;
    artifact_row record;
    fields text[];
BEGIN
    SELECT *
    INTO STRICT artifact
    FROM groundloop_m5_group_certificate_artifact
    WHERE certificate_digest = certificate_digest_to_check;
    fields := ARRAY[
        'm5-group-certificate-v1',
        'text', artifact.decision_policy_version,
        'text', artifact.group_version_id,
        'int', artifact.requirement_count::text,
        'sequence', 'int', artifact.requirement_count::text
    ];
    FOR artifact_row IN
        SELECT *
        FROM groundloop_m5_group_certificate_artifact_row
        WHERE certificate_digest = certificate_digest_to_check
        ORDER BY requirement_ordinal
    LOOP
        fields := fields || ARRAY[
            'sequence', 'int', '4',
            'int', artifact_row.requirement_ordinal::text,
            'text', artifact_row.requirement_version_id,
            'sha256', artifact_row.text_hash::text,
            'text', artifact_row.selected_observation_id
        ];
    END LOOP;
    RETURN groundloop_m5_digest_text_fields(fields);
END;
$$;

CREATE FUNCTION groundloop_m5_expected_claim_certificate(
    certificate_digest_to_check char(64)
)
RETURNS char(64)
LANGUAGE plpgsql
STABLE
STRICT
AS $$
DECLARE
    artifact record;
    fields text[];
BEGIN
    SELECT *
    INTO STRICT artifact
    FROM groundloop_m5_claim_certificate_artifact
    WHERE certificate_digest = certificate_digest_to_check;
    fields := ARRAY[
        'm5-claim-certificate-v2',
        'text', artifact.claim_id,
        'text', artifact.decision_policy_version,
        'enum', artifact.support_kind
    ];
    fields := fields || CASE
        WHEN artifact.direct_support_observation_id IS NULL THEN ARRAY['null']
        ELSE ARRAY['text', artifact.direct_support_observation_id]
    END;
    fields := fields || CASE
        WHEN artifact.group_version_id IS NULL THEN ARRAY['null']
        ELSE ARRAY['text', artifact.group_version_id]
    END;
    fields := fields || CASE
        WHEN artifact.group_certificate_digest IS NULL THEN ARRAY['null']
        ELSE ARRAY['sha256', artifact.group_certificate_digest::text]
    END;
    fields := fields || CASE
        WHEN artifact.direct_refute_observation_id IS NULL THEN ARRAY['null']
        ELSE ARRAY['text', artifact.direct_refute_observation_id]
    END;
    RETURN groundloop_m5_digest_text_fields(fields);
END;
$$;

CREATE FUNCTION groundloop_m5_assert_certificate_artifacts()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM groundloop_m5_group_certificate_artifact AS artifact
        LEFT JOIN groundloop_m5_group_certificate_artifact_row AS artifact_row
          ON artifact_row.certificate_digest = artifact.certificate_digest
        GROUP BY artifact.certificate_digest,
                 artifact.requirement_count,
                 artifact.group_version_id
        HAVING count(artifact_row.requirement_ordinal) <> artifact.requirement_count
            OR min(artifact_row.requirement_ordinal) <> 0
            OR max(artifact_row.requirement_ordinal) <> artifact.requirement_count - 1
            OR artifact.certificate_digest <>
               groundloop_m5_expected_group_certificate(artifact.certificate_digest)
    ) THEN
        RAISE EXCEPTION 'invalid M5 group certificate artifact';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM groundloop_m5_group_certificate_artifact_row AS artifact_row
        JOIN groundloop_m5_group_certificate_artifact AS artifact
          ON artifact.certificate_digest = artifact_row.certificate_digest
        JOIN groundloop_m5_requirement_version AS requirement
          ON requirement.requirement_version_id = artifact_row.requirement_version_id
        JOIN groundloop_semantic_observation AS observation
          ON observation.observation_id = artifact_row.selected_observation_id
        WHERE requirement.group_version_id <> artifact.group_version_id
           OR requirement.ordinal <> artifact_row.requirement_ordinal
           OR observation.subject_kind <> 'requirement'
           OR observation.subject_id <> artifact_row.requirement_version_id
    ) THEN
        RAISE EXCEPTION 'M5 group certificate row has cross-bound provenance';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM groundloop_m5_claim_certificate_artifact AS artifact
        LEFT JOIN groundloop_semantic_observation AS support_observation
          ON support_observation.observation_id =
             artifact.direct_support_observation_id
        LEFT JOIN groundloop_semantic_observation AS refute_observation
          ON refute_observation.observation_id =
             artifact.direct_refute_observation_id
        LEFT JOIN groundloop_m5_group_certificate_artifact AS group_certificate
          ON group_certificate.certificate_digest =
             artifact.group_certificate_digest
        LEFT JOIN groundloop_m5_group_version AS group_row
          ON group_row.group_version_id = artifact.group_version_id
        LEFT JOIN groundloop_m5_group_family AS family
          ON family.group_family_id = group_row.group_family_id
        WHERE artifact.certificate_digest <>
              groundloop_m5_expected_claim_certificate(artifact.certificate_digest)
           OR (
               artifact.direct_support_observation_id IS NOT NULL
               AND (
                   support_observation.subject_kind <> 'claim'
                   OR support_observation.subject_id <> artifact.claim_id
               )
           )
           OR (
               artifact.direct_refute_observation_id IS NOT NULL
               AND (
                   refute_observation.subject_kind <> 'claim'
                   OR refute_observation.subject_id <> artifact.claim_id
               )
           )
           OR (
               artifact.support_kind = 'group'
               AND (
                   group_certificate.group_version_id IS DISTINCT FROM
                       artifact.group_version_id
                   OR family.claim_id IS DISTINCT FROM artifact.claim_id
               )
           )
    ) THEN
        RAISE EXCEPTION 'invalid M5 claim certificate artifact';
    END IF;
    RETURN NULL;
END;
$$;

CREATE CONSTRAINT TRIGGER groundloop_m5_group_certificate_header_integrity
AFTER INSERT OR UPDATE OR DELETE ON groundloop_m5_group_certificate_artifact
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION groundloop_m5_assert_certificate_artifacts();
CREATE CONSTRAINT TRIGGER groundloop_m5_group_certificate_row_integrity
AFTER INSERT OR UPDATE OR DELETE ON groundloop_m5_group_certificate_artifact_row
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION groundloop_m5_assert_certificate_artifacts();
CREATE CONSTRAINT TRIGGER groundloop_m5_claim_certificate_integrity
AFTER INSERT OR UPDATE OR DELETE ON groundloop_m5_claim_certificate_artifact
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION groundloop_m5_assert_certificate_artifacts();

CREATE VIEW groundloop_m5_effective_working_requirement_state AS
SELECT working.epoch_id,
       working.requirement_version_id,
       working.witness_hashes,
       working.supporting_observation_ids,
       working.witness_count,
       working.satisfied,
       working.decision_policy_version,
       working.updated_revision
FROM groundloop_m5_working_requirement_state AS working
UNION ALL
SELECT update_row.epoch_id,
       published.requirement_version_id,
       published.witness_hashes,
       published.supporting_observation_ids,
       published.witness_count,
       published.satisfied,
       published.decision_policy_version,
       published.sealed_revision AS updated_revision
FROM groundloop_m5_update AS update_row
JOIN groundloop_m5_published_requirement_state AS published
  ON published.valid_from_epoch <= update_row.previous_published_epoch_id
 AND (
     published.valid_to_epoch IS NULL
     OR update_row.previous_published_epoch_id < published.valid_to_epoch
 )
WHERE NOT EXISTS (
    SELECT 1
    FROM groundloop_m5_working_requirement_state AS working
    WHERE working.epoch_id = update_row.epoch_id
      AND working.requirement_version_id = published.requirement_version_id
);

CREATE VIEW groundloop_m5_effective_working_group_state AS
SELECT working.epoch_id,
       working.group_version_id,
       working.requirement_count,
       working.satisfied_count,
       working.matching_size,
       working.complete,
       working.decision_policy_version,
       working.certificate_digest,
       working.updated_revision
FROM groundloop_m5_working_group_state AS working
UNION ALL
SELECT update_row.epoch_id,
       published.group_version_id,
       published.requirement_count,
       published.satisfied_count,
       published.matching_size,
       published.complete,
       published.decision_policy_version,
       published.certificate_digest,
       published.sealed_revision AS updated_revision
FROM groundloop_m5_update AS update_row
JOIN groundloop_m5_published_group_state AS published
  ON published.valid_from_epoch <= update_row.previous_published_epoch_id
 AND (
     published.valid_to_epoch IS NULL
     OR update_row.previous_published_epoch_id < published.valid_to_epoch
 )
WHERE NOT EXISTS (
    SELECT 1
    FROM groundloop_m5_working_group_state AS working
    WHERE working.epoch_id = update_row.epoch_id
      AND working.group_version_id = published.group_version_id
);

CREATE VIEW groundloop_m5_effective_working_claim_state AS
SELECT working.epoch_id,
       working.claim_id,
       working.support_count,
       working.refute_count,
       working.best_support_score,
       working.best_refute_score,
       working.supporting_observation_ids,
       working.refuting_observation_ids,
       working.complete_group_count,
       working.complete_group_ids,
       working.status,
       working.decision_policy_version,
       working.certificate_digest,
       working.updated_revision
FROM groundloop_m5_working_claim_state AS working
UNION ALL
SELECT update_row.epoch_id,
       published.claim_id,
       published.support_count,
       published.refute_count,
       published.best_support_score,
       published.best_refute_score,
       published.supporting_observation_ids,
       published.refuting_observation_ids,
       published.complete_group_count,
       published.complete_group_ids,
       published.status,
       published.decision_policy_version,
       published.certificate_digest,
       published.sealed_revision AS updated_revision
FROM groundloop_m5_update AS update_row
JOIN groundloop_m5_published_claim_state AS published
  ON published.valid_from_epoch <= update_row.previous_published_epoch_id
 AND (
     published.valid_to_epoch IS NULL
     OR update_row.previous_published_epoch_id < published.valid_to_epoch
 )
WHERE NOT EXISTS (
    SELECT 1
    FROM groundloop_m5_working_claim_state AS working
    WHERE working.epoch_id = update_row.epoch_id
      AND working.claim_id = published.claim_id
);

CREATE VIEW groundloop_m5_effective_working_answer_state AS
SELECT working.epoch_id,
       working.answer_version_id,
       working.required_claim_count,
       working.supported_count,
       working.unsupported_count,
       working.refuted_count,
       working.conflicted_count,
       working.status,
       working.updated_revision
FROM groundloop_m5_working_answer_state AS working
UNION ALL
SELECT update_row.epoch_id,
       published.answer_version_id,
       published.required_claim_count,
       published.supported_count,
       published.unsupported_count,
       published.refuted_count,
       published.conflicted_count,
       published.status,
       published.sealed_revision AS updated_revision
FROM groundloop_m5_update AS update_row
JOIN groundloop_m5_published_answer_state AS published
  ON published.valid_from_epoch <= update_row.previous_published_epoch_id
 AND (
     published.valid_to_epoch IS NULL
     OR update_row.previous_published_epoch_id < published.valid_to_epoch
 )
WHERE NOT EXISTS (
    SELECT 1
    FROM groundloop_m5_working_answer_state AS working
    WHERE working.epoch_id = update_row.epoch_id
      AND working.answer_version_id = published.answer_version_id
);

CREATE INDEX groundloop_m5_group_certificate_by_group_policy
    ON groundloop_m5_group_certificate_artifact(
        group_version_id, decision_policy_version, certificate_digest
    );
CREATE INDEX groundloop_m5_group_certificate_row_observation
    ON groundloop_m5_group_certificate_artifact_row(selected_observation_id);
CREATE INDEX groundloop_m5_claim_certificate_by_claim_policy
    ON groundloop_m5_claim_certificate_artifact(
        claim_id, decision_policy_version, certificate_digest
    );
CREATE INDEX groundloop_m5_current_requirement_observations
    ON groundloop_semantic_observation(
        subject_id, task_type, chunk_version_id, observation_id
    ) WHERE subject_kind = 'requirement' AND eligible_for_currency;
