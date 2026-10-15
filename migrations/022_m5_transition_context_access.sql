-- M5-D35: trusted projections only. Private transition tables/helpers stay private.
-- M5-D35-STATEMENT: create_groundloop_m5_matching_read_transition_context
CREATE FUNCTION groundloop_m5_matching_read_transition_context(
    selected_epoch_id pg_catalog.int8,
    selected_expected_revision pg_catalog.int8,
    selected_resulting_revision pg_catalog.int8,
    selected_source_kind pg_catalog.text,
    selected_source_id pg_catalog.text,
    selected_context_oid pg_catalog.oid,
    selected_journal_oid pg_catalog.oid,
    selected_expected_oid pg_catalog.oid
)
RETURNS TABLE (
    backend_pid pg_catalog.int4,
    transaction_id pg_catalog.text,
    session_role pg_catalog.text,
    epoch_id pg_catalog.int8,
    expected_revision pg_catalog.int8,
    resulting_revision pg_catalog.int8,
    source_kind pg_catalog.text,
    source_id pg_catalog.text,
    validation_started pg_catalog.bool,
    validation_done pg_catalog.bool
)
LANGUAGE plpgsql STABLE CALLED ON NULL INPUT SECURITY DEFINER
PARALLEL UNSAFE NOT LEAKPROOF SET search_path FROM CURRENT
AS $$
DECLARE
    relation_count pg_catalog.int8;
    relations_genuine pg_catalog.bool;
    owner_genuine pg_catalog.bool;
    assigned_xid pg_catalog.text;
    actual record;
BEGIN
    IF selected_epoch_id IS NULL OR selected_expected_revision IS NULL
       OR selected_resulting_revision IS NULL OR selected_source_kind IS NULL
       OR selected_source_id IS NULL OR selected_context_oid IS NULL
       OR selected_journal_oid IS NULL OR selected_expected_oid IS NULL
       OR selected_epoch_id OPERATOR(pg_catalog.<=) 0
       OR selected_expected_revision OPERATOR(pg_catalog.<=) 0
       OR selected_resulting_revision OPERATOR(pg_catalog.<=) 0
       OR selected_context_oid OPERATOR(pg_catalog.=) 0::pg_catalog.oid
       OR selected_journal_oid OPERATOR(pg_catalog.=) 0::pg_catalog.oid
       OR selected_expected_oid OPERATOR(pg_catalog.=) 0::pg_catalog.oid
       OR selected_context_oid OPERATOR(pg_catalog.=) selected_journal_oid
       OR selected_context_oid OPERATOR(pg_catalog.=) selected_expected_oid
       OR selected_journal_oid OPERATOR(pg_catalog.=) selected_expected_oid
       OR pg_catalog.btrim(selected_source_id) OPERATOR(pg_catalog.=) ''::pg_catalog.text
       OR (selected_source_kind OPERATOR(pg_catalog.=) ANY(ARRAY[
           'structural_open'::pg_catalog.text,
           'requirement_completion'::pg_catalog.text,
           'direct_transition'::pg_catalog.text])) IS NOT TRUE
    THEN RAISE EXCEPTION 'invalid transition-context arguments'; END IF;

    IF selected_source_kind OPERATOR(pg_catalog.=) 'structural_open'::pg_catalog.text
    THEN
        IF selected_expected_revision OPERATOR(pg_catalog.<>) 1
           OR selected_resulting_revision OPERATOR(pg_catalog.<>) 1
        THEN RAISE EXCEPTION 'invalid structural-open context revisions'; END IF;
    ELSIF (selected_resulting_revision::pg_catalog.numeric
           OPERATOR(pg_catalog.-) selected_expected_revision::pg_catalog.numeric
           OPERATOR(pg_catalog.=) 1::pg_catalog.numeric) IS NOT TRUE
    THEN RAISE EXCEPTION 'invalid transition-context revision edge'; END IF;

    IF (pg_catalog.current_setting('groundloop.m5_checked_transition', true)
           OPERATOR(pg_catalog.=) 'on'::pg_catalog.text) IS NOT TRUE
       OR (pg_catalog.current_setting('groundloop.m5_matching_mode', true)
           OPERATOR(pg_catalog.=) 'transition'::pg_catalog.text) IS NOT TRUE
       OR (pg_catalog.current_setting('groundloop.m5_matching_epoch_id', true)
           OPERATOR(pg_catalog.=) selected_epoch_id::pg_catalog.text) IS NOT TRUE
       OR (pg_catalog.current_setting('groundloop.m5_matching_expected_revision', true)
           OPERATOR(pg_catalog.=) selected_expected_revision::pg_catalog.text) IS NOT TRUE
       OR (pg_catalog.current_setting('groundloop.m5_matching_resulting_revision', true)
           OPERATOR(pg_catalog.=) selected_resulting_revision::pg_catalog.text) IS NOT TRUE
       OR (pg_catalog.current_setting('groundloop.m5_matching_source_kind', true)
           OPERATOR(pg_catalog.=) selected_source_kind) IS NOT TRUE
       OR (pg_catalog.current_setting('groundloop.m5_matching_source_id', true)
           OPERATOR(pg_catalog.=) selected_source_id) IS NOT TRUE
       OR (pg_catalog.current_setting('groundloop.m5_matching_context_oid', true)
           OPERATOR(pg_catalog.=) selected_context_oid::pg_catalog.text) IS NOT TRUE
       OR (pg_catalog.current_setting('groundloop.m5_matching_journal_oid', true)
           OPERATOR(pg_catalog.=) selected_journal_oid::pg_catalog.text) IS NOT TRUE
       OR (pg_catalog.current_setting('groundloop.m5_matching_expected_oid', true)
           OPERATOR(pg_catalog.=) selected_expected_oid::pg_catalog.text) IS NOT TRUE
    THEN RAISE EXCEPTION 'invalid transition-context settings'; END IF;

    IF pg_catalog.to_regclass('pg_temp.groundloop_m5_matching_promotion_context') IS NOT NULL
       OR pg_catalog.to_regclass('pg_temp.groundloop_m5_matching_promotion_journal') IS NOT NULL
       OR pg_catalog.to_regclass('pg_temp.groundloop_m5_matching_promotion_expected') IS NOT NULL
       OR (pg_catalog.to_regclass('pg_temp.groundloop_m5_matching_transition_context')::pg_catalog.oid
           OPERATOR(pg_catalog.=) selected_context_oid) IS NOT TRUE
       OR (pg_catalog.to_regclass('pg_temp.groundloop_m5_matching_change_journal')::pg_catalog.oid
           OPERATOR(pg_catalog.=) selected_journal_oid) IS NOT TRUE
       OR (pg_catalog.to_regclass('pg_temp.groundloop_m5_matching_expected_changes')::pg_catalog.oid
           OPERATOR(pg_catalog.=) selected_expected_oid) IS NOT TRUE
    THEN RAISE EXCEPTION 'transition-context triplet is absent or replaced'; END IF;

    SELECT pg_catalog.count(*), pg_catalog.bool_and(
        (r.relnamespace OPERATOR(pg_catalog.=) pg_catalog.pg_my_temp_schema())
        AND (r.relkind OPERATOR(pg_catalog.=) 'r'::pg_catalog."char")
        AND (r.relpersistence OPERATOR(pg_catalog.=) 't'::pg_catalog."char"))
    INTO relation_count, relations_genuine
    FROM pg_catalog.pg_class AS r
    WHERE r.oid OPERATOR(pg_catalog.=) ANY(ARRAY[
        selected_context_oid, selected_journal_oid, selected_expected_oid]);
    IF (relation_count OPERATOR(pg_catalog.=) 3) IS NOT TRUE
       OR relations_genuine IS NOT TRUE
    THEN RAISE EXCEPTION 'transition-context triplet is not genuine'; END IF;

    -- current_schema() is evaluated under this function's pinned trusted path.
    EXECUTE pg_catalog.format(
        'SELECT %I.groundloop_m5_matching_private_temp_triplet('
        '$1::pg_catalog.regclass,$2::pg_catalog.regclass,$3::pg_catalog.regclass)',
        pg_catalog.current_schema())
    INTO STRICT owner_genuine
    USING selected_context_oid, selected_journal_oid, selected_expected_oid;
    IF owner_genuine IS NOT TRUE
    THEN RAISE EXCEPTION 'transition-context triplet has an untrusted owner'; END IF;

    assigned_xid := pg_catalog.pg_current_xact_id_if_assigned()::pg_catalog.text;
    IF assigned_xid IS NULL
    THEN RAISE EXCEPTION 'transition-context transaction is unassigned'; END IF;
    EXECUTE $context$
        SELECT c.backend_pid, c.transaction_id::pg_catalog.text, c.session_role,
               c.epoch_id, c.expected_revision, c.resulting_revision,
               c.source_kind, c.source_id, c.validation_started,
               c.validation_done, pg_catalog.count(*) OVER () AS row_count
        FROM pg_temp.groundloop_m5_matching_transition_context AS c
    $context$ INTO STRICT actual;
    IF (actual.row_count OPERATOR(pg_catalog.=) 1) IS NOT TRUE
       OR (actual.backend_pid OPERATOR(pg_catalog.=) pg_catalog.pg_backend_pid()) IS NOT TRUE
       OR (actual.transaction_id OPERATOR(pg_catalog.=) assigned_xid) IS NOT TRUE
       OR (actual.session_role OPERATOR(pg_catalog.=) session_user::pg_catalog.text) IS NOT TRUE
       OR (actual.epoch_id OPERATOR(pg_catalog.=) selected_epoch_id) IS NOT TRUE
       OR (actual.expected_revision OPERATOR(pg_catalog.=) selected_expected_revision) IS NOT TRUE
       OR (actual.resulting_revision OPERATOR(pg_catalog.=) selected_resulting_revision) IS NOT TRUE
       OR (actual.source_kind OPERATOR(pg_catalog.=) selected_source_kind) IS NOT TRUE
       OR (actual.source_id OPERATOR(pg_catalog.=) selected_source_id) IS NOT TRUE
       OR actual.validation_started IS NOT FALSE OR actual.validation_done IS NOT FALSE
    THEN RAISE EXCEPTION 'transition-context row is invalid'; END IF;
    RETURN QUERY SELECT actual.backend_pid, actual.transaction_id, actual.session_role,
        actual.epoch_id, actual.expected_revision, actual.resulting_revision,
        actual.source_kind, actual.source_id, actual.validation_started, actual.validation_done;
END;
$$;

-- M5-D35-STATEMENT: revoke_groundloop_m5_matching_read_transition_context
REVOKE ALL ON FUNCTION groundloop_m5_matching_read_transition_context(
    pg_catalog.int8,pg_catalog.int8,pg_catalog.int8,pg_catalog.text,
    pg_catalog.text,pg_catalog.oid,pg_catalog.oid,pg_catalog.oid) FROM PUBLIC;
-- M5-D35-STATEMENT: grant_groundloop_m5_matching_read_transition_context
GRANT EXECUTE ON FUNCTION groundloop_m5_matching_read_transition_context(
    pg_catalog.int8,pg_catalog.int8,pg_catalog.int8,pg_catalog.text,
    pg_catalog.text,pg_catalog.oid,pg_catalog.oid,pg_catalog.oid) TO PUBLIC;

-- M5-D35-STATEMENT: create_groundloop_m5_matching_read_transition_stage_journal
CREATE FUNCTION groundloop_m5_matching_read_transition_stage_journal(
    selected_epoch_id pg_catalog.int8,
    selected_expected_revision pg_catalog.int8,
    selected_resulting_revision pg_catalog.int8,
    selected_source_kind pg_catalog.text,
    selected_source_id pg_catalog.text,
    selected_context_oid pg_catalog.oid,
    selected_journal_oid pg_catalog.oid,
    selected_expected_oid pg_catalog.oid
)
RETURNS TABLE (
    relation_name pg_catalog.text, key_preimage pg_catalog.bytea,
    first_old pg_catalog.jsonb, final_new pg_catalog.jsonb,
    first_operation pg_catalog.text, last_operation pg_catalog.text,
    mutation_count pg_catalog.int4, saw_insert pg_catalog.bool,
    saw_update pg_catalog.bool, saw_delete pg_catalog.bool
)
LANGUAGE plpgsql STABLE CALLED ON NULL INPUT SECURITY DEFINER
PARALLEL UNSAFE NOT LEAKPROOF SET search_path FROM CURRENT
AS $$
DECLARE attested record;
BEGIN
    EXECUTE pg_catalog.format(
        'SELECT * FROM %I.groundloop_m5_matching_read_transition_context('
        '$1::pg_catalog.int8,$2::pg_catalog.int8,$3::pg_catalog.int8,'
        '$4::pg_catalog.text,$5::pg_catalog.text,$6::pg_catalog.oid,'
        '$7::pg_catalog.oid,$8::pg_catalog.oid)', pg_catalog.current_schema())
    INTO STRICT attested
    USING selected_epoch_id, selected_expected_revision, selected_resulting_revision,
          selected_source_kind, selected_source_id, selected_context_oid,
          selected_journal_oid, selected_expected_oid;
    RETURN QUERY SELECT j.relation_name, j.key_preimage, j.first_old, j.final_new,
        j.first_operation, j.last_operation, j.mutation_count,
        j.saw_insert, j.saw_update, j.saw_delete
    FROM pg_temp.groundloop_m5_matching_change_journal AS j
    WHERE j.relation_name OPERATOR(pg_catalog.=) ANY(ARRAY[
        'groundloop_m5_matching_image_working'::pg_catalog.text,
        'groundloop_m5_matching_observation_working'::pg_catalog.text,
        'groundloop_m5_matching_edge_working'::pg_catalog.text,
        'groundloop_m5_matching_hash_mask_working'::pg_catalog.text,
        'groundloop_m5_matching_hall_working'::pg_catalog.text,
        'groundloop_m5_working_requirement_state'::pg_catalog.text,
        'groundloop_m5_working_group_state'::pg_catalog.text,
        'groundloop_m5_working_claim_state'::pg_catalog.text,
        'groundloop_m5_working_answer_state'::pg_catalog.text,
        'groundloop_m5_group_certificate_artifact'::pg_catalog.text,
        'groundloop_m5_group_certificate_artifact_row'::pg_catalog.text,
        'groundloop_m5_claim_certificate_artifact'::pg_catalog.text,
        'groundloop_m5_working_group_certificate_binding'::pg_catalog.text,
        'groundloop_m5_working_claim_certificate_binding'::pg_catalog.text]);
END;
$$;

-- M5-D35-STATEMENT: revoke_groundloop_m5_matching_read_transition_stage_journal
REVOKE ALL ON FUNCTION groundloop_m5_matching_read_transition_stage_journal(
    pg_catalog.int8,pg_catalog.int8,pg_catalog.int8,pg_catalog.text,
    pg_catalog.text,pg_catalog.oid,pg_catalog.oid,pg_catalog.oid) FROM PUBLIC;
-- M5-D35-STATEMENT: grant_groundloop_m5_matching_read_transition_stage_journal
GRANT EXECUTE ON FUNCTION groundloop_m5_matching_read_transition_stage_journal(
    pg_catalog.int8,pg_catalog.int8,pg_catalog.int8,pg_catalog.text,
    pg_catalog.text,pg_catalog.oid,pg_catalog.oid,pg_catalog.oid) TO PUBLIC;
