CREATE FUNCTION groundloop_m5_matching_read_preterminal_seal_context(
    selected_epoch_id bigint,
    selected_expected_revision bigint,
    selected_sealed_revision bigint
)
RETURNS TABLE (
    policy_version text,
    anchor_m4_epoch_id bigint,
    anchor_m5_epoch_id bigint,
    anchor_m5_revision bigint,
    anchor_activation_count integer,
    anchor_predecessor_revision bigint,
    anchor_predecessor_sealed_at timestamptz,
    anchor_current_policy text
)
LANGUAGE plpgsql
STABLE
CALLED ON NULL INPUT
SECURITY DEFINER
PARALLEL UNSAFE
NOT LEAKPROOF
SET search_path FROM CURRENT
AS $$
DECLARE
    checked_transition_setting text;
    mode_setting text;
    epoch_setting text;
    expected_revision_setting text;
    resulting_revision_setting text;
    policy_setting text;
    context_oid_setting text;
    journal_oid_setting text;
    expected_oid_setting text;
    transition_context_relation regclass;
    transition_journal_relation regclass;
    transition_expected_relation regclass;
    promotion_context_relation regclass;
    promotion_journal_relation regclass;
    promotion_expected_relation regclass;
    relation_count bigint;
    distinct_relation_count bigint;
    relations_are_genuine boolean;
    context_backend_pid integer;
    context_transaction_id bigint;
    context_session_role text;
    context_mode text;
    context_epoch_id bigint;
    context_expected_revision bigint;
    context_resulting_revision bigint;
    context_policy_version text;
    context_validation_started boolean;
    context_validation_done boolean;
    context_row_count bigint;
BEGIN
    IF selected_epoch_id IS NULL
       OR selected_expected_revision IS NULL
       OR selected_sealed_revision IS NULL
       OR selected_epoch_id <= 0
       OR selected_expected_revision <= 0
       OR selected_sealed_revision <= 0
       OR selected_sealed_revision::numeric
            - selected_expected_revision::numeric IS DISTINCT FROM 1::numeric
    THEN
        RAISE EXCEPTION 'invalid preterminal seal-context coordinates';
    END IF;

    checked_transition_setting :=
        current_setting('groundloop.m5_checked_transition', true);
    mode_setting := current_setting('groundloop.m5_matching_mode', true);
    epoch_setting :=
        current_setting('groundloop.m5_matching_epoch_id', true);
    expected_revision_setting :=
        current_setting('groundloop.m5_matching_expected_revision', true);
    resulting_revision_setting :=
        current_setting('groundloop.m5_matching_resulting_revision', true);
    policy_setting := current_setting('groundloop.m5_matching_policy', true);
    context_oid_setting :=
        current_setting('groundloop.m5_matching_context_oid', true);
    journal_oid_setting :=
        current_setting('groundloop.m5_matching_journal_oid', true);
    expected_oid_setting :=
        current_setting('groundloop.m5_matching_expected_oid', true);

    IF checked_transition_setting IS DISTINCT FROM 'on'
       OR mode_setting IS DISTINCT FROM 'seal'
       OR epoch_setting::bigint IS DISTINCT FROM selected_epoch_id
       OR expected_revision_setting::bigint
            IS DISTINCT FROM selected_expected_revision
       OR resulting_revision_setting::bigint
            IS DISTINCT FROM selected_sealed_revision
       OR policy_setting IS NULL
       OR policy_setting = ''
    THEN
        RAISE EXCEPTION 'invalid preterminal seal-context binding';
    END IF;

    transition_context_relation :=
        to_regclass('pg_temp.groundloop_m5_matching_transition_context');
    transition_journal_relation :=
        to_regclass('pg_temp.groundloop_m5_matching_change_journal');
    transition_expected_relation :=
        to_regclass('pg_temp.groundloop_m5_matching_expected_changes');
    IF transition_context_relation IS NOT NULL
       OR transition_journal_relation IS NOT NULL
       OR transition_expected_relation IS NOT NULL
    THEN
        RAISE EXCEPTION 'transition context conflicts with preterminal seal context';
    END IF;

    promotion_context_relation :=
        to_regclass('pg_temp.groundloop_m5_matching_promotion_context');
    promotion_journal_relation :=
        to_regclass('pg_temp.groundloop_m5_matching_promotion_journal');
    promotion_expected_relation :=
        to_regclass('pg_temp.groundloop_m5_matching_promotion_expected');

    IF promotion_context_relation IS NULL
       OR promotion_journal_relation IS NULL
       OR promotion_expected_relation IS NULL
    THEN
        RAISE EXCEPTION 'preterminal promotion context is incomplete';
    END IF;

    SELECT count(*),
           count(DISTINCT relation_row.oid),
           bool_and(
               relation_row.relkind = 'r'
               AND relation_row.relpersistence = 't'
               AND relation_row.relnamespace = pg_my_temp_schema()
           )
    INTO relation_count, distinct_relation_count, relations_are_genuine
    FROM unnest(
        ARRAY[
            promotion_context_relation,
            promotion_journal_relation,
            promotion_expected_relation
        ]
    ) AS expected_relation(relation_oid)
    JOIN pg_catalog.pg_class AS relation_row
      ON relation_row.oid = expected_relation.relation_oid;

    IF relation_count IS DISTINCT FROM 3
       OR distinct_relation_count IS DISTINCT FROM 3
       OR relations_are_genuine IS DISTINCT FROM true
       OR groundloop_m5_matching_private_temp_triplet(
              promotion_context_relation,
              promotion_journal_relation,
              promotion_expected_relation
          ) IS DISTINCT FROM true
    THEN
        RAISE EXCEPTION 'preterminal promotion relations are not genuine';
    END IF;

    IF promotion_context_relation::oid::text
            IS DISTINCT FROM context_oid_setting
       OR promotion_journal_relation::oid::text
            IS DISTINCT FROM journal_oid_setting
       OR promotion_expected_relation::oid::text
            IS DISTINCT FROM expected_oid_setting
    THEN
        RAISE EXCEPTION 'preterminal promotion relation identity changed';
    END IF;

    EXECUTE $context$
        SELECT backend_pid,
               transaction_id,
               session_role,
               mode,
               epoch_id,
               expected_revision,
               resulting_revision,
               policy_version,
               anchor_m4_epoch_id,
               anchor_m5_epoch_id,
               anchor_m5_revision,
               anchor_activation_count,
               anchor_predecessor_revision,
               anchor_predecessor_sealed_at,
               anchor_current_policy,
               validation_started,
               validation_done,
               count(*) OVER ()
        FROM pg_temp.groundloop_m5_matching_promotion_context
    $context$
    INTO STRICT
        context_backend_pid,
        context_transaction_id,
        context_session_role,
        context_mode,
        context_epoch_id,
        context_expected_revision,
        context_resulting_revision,
        context_policy_version,
        anchor_m4_epoch_id,
        anchor_m5_epoch_id,
        anchor_m5_revision,
        anchor_activation_count,
        anchor_predecessor_revision,
        anchor_predecessor_sealed_at,
        anchor_current_policy,
        context_validation_started,
        context_validation_done,
        context_row_count;

    IF context_row_count IS DISTINCT FROM 1
       OR context_backend_pid IS DISTINCT FROM pg_backend_pid()
       OR context_transaction_id
            IS DISTINCT FROM pg_current_xact_id()::text::bigint
       OR context_session_role IS DISTINCT FROM session_user
       OR context_mode IS DISTINCT FROM 'seal'
       OR context_epoch_id IS DISTINCT FROM selected_epoch_id
       OR context_expected_revision IS DISTINCT FROM selected_expected_revision
       OR context_resulting_revision IS DISTINCT FROM selected_sealed_revision
       OR context_policy_version IS DISTINCT FROM policy_setting
       OR context_validation_started IS DISTINCT FROM false
       OR context_validation_done IS DISTINCT FROM false
    THEN
        RAISE EXCEPTION 'preterminal promotion context row is invalid';
    END IF;

    policy_version := context_policy_version;
    RETURN NEXT;
END;
$$;

REVOKE ALL ON FUNCTION groundloop_m5_matching_read_preterminal_seal_context(
    bigint,
    bigint,
    bigint
) FROM PUBLIC;

GRANT EXECUTE ON FUNCTION groundloop_m5_matching_read_preterminal_seal_context(
    bigint,
    bigint,
    bigint
) TO PUBLIC;
