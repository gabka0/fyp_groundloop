-- Exact open-work counters for point-bounded M4 measured coordination.

ALTER TABLE groundloop_epoch
    ADD COLUMN open_job_count bigint NOT NULL DEFAULT 0
        CHECK (open_job_count >= 0),
    ADD COLUMN open_scope_count bigint NOT NULL DEFAULT 0
        CHECK (open_scope_count >= 0);

UPDATE groundloop_epoch AS epoch
SET open_job_count = counts.open_job_count
FROM (
    SELECT epoch_id, count(*)::bigint AS open_job_count
    FROM groundloop_semantic_job
    WHERE job_state NOT IN ('completed_active', 'completed_inactive')
    GROUP BY epoch_id
) AS counts
WHERE counts.epoch_id = epoch.epoch_id;

UPDATE groundloop_epoch AS epoch
SET open_scope_count = counts.open_scope_count
FROM (
    SELECT epoch_id, count(*)::bigint AS open_scope_count
    FROM groundloop_discovery_scope
    WHERE closed_revision IS NULL
    GROUP BY epoch_id
) AS counts
WHERE counts.epoch_id = epoch.epoch_id;

CREATE FUNCTION groundloop_adjust_open_job_count()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    old_open integer := 0;
    new_open integer := 0;
    target_epoch_id bigint;
BEGIN
    IF TG_OP <> 'INSERT' THEN
        old_open := CASE
            WHEN OLD.job_state NOT IN ('completed_active', 'completed_inactive')
                THEN 1
            ELSE 0
        END;
    END IF;
    IF TG_OP <> 'DELETE' THEN
        new_open := CASE
            WHEN NEW.job_state NOT IN ('completed_active', 'completed_inactive')
                THEN 1
            ELSE 0
        END;
    END IF;
    target_epoch_id := CASE
        WHEN TG_OP = 'DELETE' THEN OLD.epoch_id
        ELSE NEW.epoch_id
    END;
    IF new_open <> old_open THEN
        UPDATE groundloop_epoch
        SET open_job_count = open_job_count + new_open - old_open
        WHERE epoch_id = target_epoch_id;
    END IF;
    RETURN CASE WHEN TG_OP = 'DELETE' THEN OLD ELSE NEW END;
END;
$$;

CREATE TRIGGER groundloop_semantic_job_open_count
AFTER INSERT OR DELETE OR UPDATE OF job_state ON groundloop_semantic_job
FOR EACH ROW EXECUTE FUNCTION groundloop_adjust_open_job_count();

CREATE FUNCTION groundloop_adjust_open_scope_count()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    old_open integer := 0;
    new_open integer := 0;
    target_epoch_id bigint;
BEGIN
    IF TG_OP <> 'INSERT' THEN
        old_open := CASE WHEN OLD.closed_revision IS NULL THEN 1 ELSE 0 END;
    END IF;
    IF TG_OP <> 'DELETE' THEN
        new_open := CASE WHEN NEW.closed_revision IS NULL THEN 1 ELSE 0 END;
    END IF;
    target_epoch_id := CASE
        WHEN TG_OP = 'DELETE' THEN OLD.epoch_id
        ELSE NEW.epoch_id
    END;
    IF new_open <> old_open THEN
        UPDATE groundloop_epoch
        SET open_scope_count = open_scope_count + new_open - old_open
        WHERE epoch_id = target_epoch_id;
    END IF;
    RETURN CASE WHEN TG_OP = 'DELETE' THEN OLD ELSE NEW END;
END;
$$;

CREATE TRIGGER groundloop_discovery_scope_open_count
AFTER INSERT OR DELETE OR UPDATE OF closed_revision ON groundloop_discovery_scope
FOR EACH ROW EXECUTE FUNCTION groundloop_adjust_open_scope_count();
