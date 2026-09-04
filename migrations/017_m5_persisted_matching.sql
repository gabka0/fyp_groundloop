-- GroundLoop M5-D25 persisted matching schema.
-- Installed only by install_m5_persisted_matching_bundle().

-- groundloop:m5-persisted-matching-group:helpers
CREATE FUNCTION groundloop_m5_matching_sha256(value char(64))
RETURNS boolean LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE
AS $$ SELECT value ~ '^[0-9a-f]{64}$' $$;

-- Frozen to the 29 code points for which the accepted Python runtime's
-- str.strip() makes an otherwise empty identifier empty.
CREATE FUNCTION groundloop_m5_matching_identifier(value text)
RETURNS boolean LANGUAGE plpgsql IMMUTABLE STRICT PARALLEL SAFE AS $$
DECLARE position integer; codepoint integer;
BEGIN
  IF value='' THEN RETURN false; END IF;
  FOR position IN 1..char_length(value) LOOP
    codepoint:=ascii(substr(value,position,1));
    IF codepoint NOT IN (
      9,10,11,12,13,28,29,30,31,32,133,160,5760,
      8192,8193,8194,8195,8196,8197,8198,8199,8200,8201,8202,
      8232,8233,8239,8287,12288
    ) THEN RETURN true; END IF;
  END LOOP;
  RETURN false;
END;
$$;

CREATE FUNCTION groundloop_m5_matching_digest_text_fields(fields_to_hash text[])
RETURNS char(64) LANGUAGE plpgsql IMMUTABLE STRICT PARALLEL SAFE AS $$
DECLARE field text;
DECLARE encoded bytea;
DECLARE payload bytea:=''::bytea;
BEGIN
  FOREACH field IN ARRAY fields_to_hash LOOP
    IF field IS NULL THEN
      RAISE EXCEPTION 'persisted matching digest fields cannot contain SQL NULL';
    END IF;
    encoded:=convert_to(field,'UTF8');
    payload:=payload||int8send(octet_length(encoded)::bigint)||encoded;
  END LOOP;
  RETURN encode(public.digest(payload,'sha256'),'hex');
END;
$$;

CREATE FUNCTION groundloop_m5_matching_decode_preimage(value bytea)
RETURNS text[] LANGUAGE plpgsql IMMUTABLE STRICT PARALLEL SAFE AS $$
DECLARE
    offset_value integer := 0;
    total integer := octet_length(value);
    field_length numeric;
    step integer;
    encoded bytea;
    fields text[] := ARRAY[]::text[];
BEGIN
    WHILE offset_value < total LOOP
        IF total - offset_value < 8 THEN
            RAISE EXCEPTION 'truncated persisted-matching frame length';
        END IF;
        field_length := 0;
        FOR step IN 0..7 LOOP
            field_length := field_length * 256 + get_byte(value, offset_value + step);
        END LOOP;
        IF field_length > 2147483647 OR field_length > total-offset_value-8 THEN
            RAISE EXCEPTION 'invalid persisted-matching frame length';
        END IF;
        encoded := substring(value FROM offset_value + 9 FOR field_length::integer);
        fields := fields || convert_from(encoded, 'UTF8');
        offset_value := offset_value + 8 + field_length::integer;
    END LOOP;
    IF offset_value <> total OR cardinality(fields)=0 THEN
        RAISE EXCEPTION 'invalid persisted-matching framed preimage';
    END IF;
    IF groundloop_m5_matching_digest_text_fields(fields) <>
       encode(public.digest(value, 'sha256'), 'hex') THEN
        RAISE EXCEPTION 'persisted-matching preimage is not canonical framing';
    END IF;
    RETURN fields;
END;
$$;

CREATE FUNCTION groundloop_m5_matching_hash_preimage(value bytea)
RETURNS char(64) LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE AS $$
  SELECT groundloop_m5_matching_digest_text_fields(
    groundloop_m5_matching_decode_preimage(value)
  )
$$;

CREATE FUNCTION groundloop_m5_matching_parse_field(fields text[], position_value integer)
RETURNS jsonb LANGUAGE plpgsql IMMUTABLE STRICT PARALLEL SAFE AS $$
DECLARE tag text; value text; count_value integer; cursor_value integer; index_value integer;
DECLARE children jsonb := '[]'::jsonb; child jsonb;
DECLARE scalar_bytes bytea;
BEGIN
  IF position_value<1 OR position_value>cardinality(fields) THEN
    RAISE EXCEPTION 'missing persisted-matching typed field';
  END IF;
  tag:=fields[position_value];
  IF tag='null' THEN
    RETURN jsonb_build_object('next',position_value+1,'tag','null');
  END IF;
  IF tag IN ('text','enum','sha256','int','bool','f64') THEN
    IF position_value+1>cardinality(fields) THEN RAISE EXCEPTION 'truncated typed scalar'; END IF;
    value:=fields[position_value+1];
    IF tag IN ('text','enum') AND value='' THEN RAISE EXCEPTION 'empty typed text'; END IF;
    IF tag='sha256' AND value!~'^[0-9a-f]{64}$' THEN RAISE EXCEPTION 'invalid typed hash'; END IF;
    IF tag='int' AND value!~'^(0|-?[1-9][0-9]*)$' THEN RAISE EXCEPTION 'noncanonical typed integer'; END IF;
    IF tag='bool' AND value NOT IN ('0','1') THEN RAISE EXCEPTION 'invalid typed boolean'; END IF;
    IF tag='f64' AND value!~'^[0-9a-f]{16}$' THEN RAISE EXCEPTION 'invalid typed binary64'; END IF;
    IF tag='f64' THEN
      scalar_bytes:=decode(value,'hex');
      IF (get_byte(scalar_bytes,0)&127)=127 AND (get_byte(scalar_bytes,1)&240)=240 THEN
        RAISE EXCEPTION 'nonfinite typed binary64';
      END IF;
    END IF;
    RETURN jsonb_build_object('next',position_value+2,'tag',tag,'value',value);
  END IF;
  IF tag='sequence' THEN
    IF position_value+2>cardinality(fields) OR fields[position_value+1]<>'int'
       OR fields[position_value+2]!~'^(0|[1-9][0-9]*)$' THEN
      RAISE EXCEPTION 'invalid typed sequence count';
    END IF;
    count_value:=fields[position_value+2]::integer; cursor_value:=position_value+3;
    FOR index_value IN 1..count_value LOOP
      child:=groundloop_m5_matching_parse_field(fields,cursor_value);
      children:=children||jsonb_build_array(child-'next');
      cursor_value:=(child->>'next')::integer;
    END LOOP;
    RETURN jsonb_build_object('next',cursor_value,'tag','sequence','children',children);
  END IF;
  RAISE EXCEPTION 'unknown persisted-matching typed tag %',tag;
EXCEPTION WHEN numeric_value_out_of_range THEN
  RAISE EXCEPTION 'typed count exceeds PostgreSQL decoder bound';
END;
$$;

CREATE FUNCTION groundloop_m5_matching_parse_typed_preimage(
  value bytea, expected_domain text
) RETURNS jsonb LANGUAGE plpgsql IMMUTABLE PARALLEL SAFE AS $$
DECLARE fields text[]:=groundloop_m5_matching_decode_preimage(value);
DECLARE cursor_value integer:=2; child jsonb; children jsonb:='[]'::jsonb;
BEGIN
  IF fields[1]<>expected_domain THEN RAISE EXCEPTION 'typed preimage domain mismatch'; END IF;
  WHILE cursor_value<=cardinality(fields) LOOP
    child:=groundloop_m5_matching_parse_field(fields,cursor_value);
    children:=children||jsonb_build_array(child-'next');
    cursor_value:=(child->>'next')::integer;
  END LOOP;
  IF cursor_value<>cardinality(fields)+1 THEN RAISE EXCEPTION 'typed preimage trailing field'; END IF;
  RETURN jsonb_build_object('domain',expected_domain,'children',children);
END;
$$;

CREATE FUNCTION groundloop_m5_matching_u64(value bytea, offset_value integer)
RETURNS bigint LANGUAGE plpgsql IMMUTABLE STRICT PARALLEL SAFE AS $$
DECLARE result_value numeric:=0; step integer;
BEGIN
  IF offset_value<0 OR octet_length(value)-offset_value<8 THEN
    RAISE EXCEPTION 'truncated logical uint64';
  END IF;
  FOR step IN 0..7 LOOP result_value:=result_value*256+get_byte(value,offset_value+step); END LOOP;
  IF result_value>9223372036854775807 THEN RAISE EXCEPTION 'logical uint64 exceeds decoder bound'; END IF;
  RETURN result_value::bigint;
END;
$$;

CREATE FUNCTION groundloop_m5_matching_parse_logical(value bytea)
RETURNS jsonb LANGUAGE plpgsql IMMUTABLE STRICT PARALLEL SAFE AS $$
DECLARE tag integer; payload_length bigint; cursor_value integer; count_value bigint;
DECLARE child_bytes bytea; child jsonb; children jsonb:='[]'::jsonb;
DECLARE name_value text; names text[]; field_name text; step bigint;
BEGIN
  IF octet_length(value)<1 THEN RAISE EXCEPTION 'empty logical value'; END IF;
  tag:=get_byte(value,0);
  IF tag=ascii('n') THEN
    IF octet_length(value)<>1 THEN RAISE EXCEPTION 'logical None trailing byte'; END IF;
    RETURN jsonb_build_object('tag','none');
  ELSIF tag=ascii('b') THEN
    IF octet_length(value)<>2 OR get_byte(value,1) NOT IN (0,1) THEN RAISE EXCEPTION 'invalid logical boolean'; END IF;
    RETURN jsonb_build_object('tag','bool','value',get_byte(value,1)=1);
  ELSIF tag IN (ascii('e'),ascii('s'),ascii('i')) THEN
    payload_length:=groundloop_m5_matching_u64(value,1);
    IF payload_length>2147483647 OR octet_length(value)<>9+payload_length THEN RAISE EXCEPTION 'logical scalar length mismatch'; END IF;
    name_value:=convert_from(substring(value FROM 10 FOR payload_length::integer),'UTF8');
    IF tag=ascii('i') AND name_value!~'^(0|-?[1-9][0-9]*)$' THEN RAISE EXCEPTION 'noncanonical logical integer'; END IF;
    IF tag=ascii('e') AND name_value='' THEN RAISE EXCEPTION 'empty logical enum'; END IF;
    RETURN jsonb_build_object('tag',CASE tag WHEN ascii('e') THEN 'enum' WHEN ascii('s') THEN 'str' ELSE 'int' END,'value',name_value);
  ELSIF tag=ascii('f') THEN
    IF octet_length(value)<>9 OR (get_byte(value,1)&127)=127 AND (get_byte(value,2)&240)=240 THEN
      RAISE EXCEPTION 'invalid or nonfinite logical binary64';
    END IF;
    RETURN jsonb_build_object('tag','f64','value',encode(substring(value FROM 2),'hex'));
  ELSIF tag=ascii('q') THEN
    count_value:=groundloop_m5_matching_u64(value,1); cursor_value:=9;
    FOR step IN 1..count_value LOOP
      payload_length:=groundloop_m5_matching_u64(value,cursor_value); cursor_value:=cursor_value+8;
      IF payload_length>2147483647 OR payload_length>octet_length(value)-cursor_value THEN RAISE EXCEPTION 'logical tuple frame overrun'; END IF;
      child_bytes:=substring(value FROM cursor_value+1 FOR payload_length::integer);
      child:=groundloop_m5_matching_parse_logical(child_bytes);
      children:=children||jsonb_build_array(child); cursor_value:=cursor_value+payload_length::integer;
    END LOOP;
    IF cursor_value<>octet_length(value) THEN RAISE EXCEPTION 'logical tuple trailing byte'; END IF;
    RETURN jsonb_build_object('tag','tuple','children',children);
  ELSIF tag=ascii('d') THEN
    payload_length:=groundloop_m5_matching_u64(value,1);
    IF payload_length>2147483647 OR payload_length>octet_length(value)-9 THEN RAISE EXCEPTION 'logical dataclass name overrun'; END IF;
    name_value:=convert_from(substring(value FROM 10 FOR payload_length::integer),'UTF8');
    cursor_value:=9+payload_length::integer; count_value:=groundloop_m5_matching_u64(value,cursor_value); cursor_value:=cursor_value+8;
    names:=CASE name_value
      WHEN 'RequirementState' THEN ARRAY['requirement_version_id','witness_hashes','supporting_observation_ids','witness_count','satisfied']
      WHEN 'GroupState' THEN ARRAY['group_version_id','requirement_count','satisfied_count','matching_size','complete']
      WHEN 'CombinedClaimState' THEN ARRAY['claim_id','support_count','refute_count','best_support_score','best_refute_score','supporting_observation_ids','refuting_observation_ids','complete_group_count','complete_group_ids','status']
      WHEN 'CombinedAnswerState' THEN ARRAY['answer_version_id','required_claim_count','supported_count','unsupported_count','refuted_count','conflicted_count','status']
      WHEN 'GroupCertificateRow' THEN ARRAY['requirement_ordinal','requirement_version_id','text_hash','selected_observation_id']
      WHEN 'GroupMatchingCertificateArtifact' THEN ARRAY['decision_policy_version','group_version_id','rows','certificate_version','certificate_digest']
      WHEN 'ClaimCertificateArtifact' THEN ARRAY['claim_id','decision_policy_version','support_kind','direct_support_observation_id','group_version_id','group_certificate_digest','direct_refute_observation_id','certificate_version','certificate_digest']
      WHEN 'WorkingGroupCertificateBinding' THEN ARRAY['epoch_id','group_version_id','valid_from_revision','valid_to_revision','certificate_digest']
      WHEN 'WorkingClaimCertificateBinding' THEN ARRAY['epoch_id','claim_id','valid_from_revision','valid_to_revision','certificate_digest']
      WHEN 'StatusDelta' THEN ARRAY['event_id','object_type','object_id','old_status','new_status','reason']
      ELSE NULL END;
    IF names IS NULL OR count_value<>cardinality(names) THEN RAISE EXCEPTION 'unknown or malformed logical dataclass'; END IF;
    FOR step IN 1..count_value LOOP
      payload_length:=groundloop_m5_matching_u64(value,cursor_value); cursor_value:=cursor_value+8;
      IF payload_length>2147483647 OR payload_length>octet_length(value)-cursor_value THEN RAISE EXCEPTION 'logical field-name overrun'; END IF;
      field_name:=convert_from(substring(value FROM cursor_value+1 FOR payload_length::integer),'UTF8'); cursor_value:=cursor_value+payload_length::integer;
      IF field_name<>names[step] THEN RAISE EXCEPTION 'logical dataclass field order mismatch'; END IF;
      payload_length:=groundloop_m5_matching_u64(value,cursor_value); cursor_value:=cursor_value+8;
      IF payload_length>2147483647 OR payload_length>octet_length(value)-cursor_value THEN RAISE EXCEPTION 'logical field-value overrun'; END IF;
      child:=groundloop_m5_matching_parse_logical(substring(value FROM cursor_value+1 FOR payload_length::integer));
      children:=children||jsonb_build_array(jsonb_build_object('name',field_name,'value',child)); cursor_value:=cursor_value+payload_length::integer;
    END LOOP;
    IF cursor_value<>octet_length(value) THEN RAISE EXCEPTION 'logical dataclass trailing byte'; END IF;
    RETURN jsonb_build_object('tag','dataclass','name',name_value,'fields',children);
  END IF;
  RAISE EXCEPTION 'unknown logical tag %',tag;
END;
$$;

CREATE FUNCTION groundloop_m5_matching_reencode_logical(node jsonb)
RETURNS bytea LANGUAGE plpgsql IMMUTABLE STRICT PARALLEL SAFE AS $$
DECLARE tag_value text:=node->>'tag'; result_value bytea; payload bytea;
DECLARE child jsonb; field_value jsonb;
BEGIN
  IF tag_value='none' THEN RETURN decode('6e','hex');
  ELSIF tag_value='bool' THEN
    RETURN decode(CASE WHEN (node->>'value')::boolean THEN '6201' ELSE '6200' END,'hex');
  ELSIF tag_value IN ('enum','str','int') THEN
    payload:=convert_to(node->>'value','UTF8');
    RETURN convert_to(CASE tag_value WHEN 'enum' THEN 'e' WHEN 'str' THEN 's' ELSE 'i' END,'UTF8')
      || int8send(octet_length(payload)) || payload;
  ELSIF tag_value='f64' THEN RETURN convert_to('f','UTF8')||decode(node->>'value','hex');
  ELSIF tag_value='tuple' THEN
    result_value:=convert_to('q','UTF8')||int8send(jsonb_array_length(node->'children'));
    FOR child IN SELECT entry.value FROM jsonb_array_elements(node->'children') entry(value) LOOP
      payload:=groundloop_m5_matching_reencode_logical(child);
      result_value:=result_value||int8send(octet_length(payload))||payload;
    END LOOP;
    RETURN result_value;
  ELSIF tag_value='dataclass' THEN
    payload:=convert_to(node->>'name','UTF8');
    result_value:=convert_to('d','UTF8')||int8send(octet_length(payload))||payload
      ||int8send(jsonb_array_length(node->'fields'));
    FOR field_value IN SELECT entry.value FROM jsonb_array_elements(node->'fields') entry(value) LOOP
      payload:=convert_to(field_value->>'name','UTF8');
      result_value:=result_value||int8send(octet_length(payload))||payload;
      payload:=groundloop_m5_matching_reencode_logical(field_value->'value');
      result_value:=result_value||int8send(octet_length(payload))||payload;
    END LOOP;
    RETURN result_value;
  END IF;
  RAISE EXCEPTION 'cannot re-encode unknown logical tag %',tag_value;
END;
$$;

CREATE FUNCTION groundloop_m5_matching_validate_logical_dataclass(node jsonb)
RETURNS boolean LANGUAGE plpgsql IMMUTABLE STRICT PARALLEL SAFE AS $$
DECLARE name_value text:=node->>'name'; values_array jsonb:=node->'fields';
DECLARE expected text[]; item jsonb; position integer;
DECLARE rows_node jsonb; row_node jsonb; digest_fields text[]; expected_digest text;
DECLARE row_index integer:=0; requirement_ids text[]:=ARRAY[]::text[];
DECLARE text_hashes text[]:=ARRAY[]::text[]; support_kind_value text;
BEGIN
  expected:=CASE name_value
    WHEN 'RequirementState' THEN ARRAY['str','tuple:str','tuple:str','int','bool']
    WHEN 'GroupState' THEN ARRAY['str','int','int','int','bool']
    WHEN 'CombinedClaimState' THEN ARRAY['str','int','int','option:f64','option:f64','tuple:str','tuple:str','int','tuple:str','enum']
    WHEN 'CombinedAnswerState' THEN ARRAY['str','int','int','int','int','int','enum']
    WHEN 'GroupCertificateRow' THEN ARRAY['int','str','str','str']
    WHEN 'GroupMatchingCertificateArtifact' THEN ARRAY['str','str','tuple:GroupCertificateRow','str','str']
    WHEN 'ClaimCertificateArtifact' THEN ARRAY['str','str','enum','option:str','option:str','option:str','option:str','str','str']
    WHEN 'WorkingGroupCertificateBinding' THEN ARRAY['int','str','int','option:int','str']
    WHEN 'WorkingClaimCertificateBinding' THEN ARRAY['int','str','int','option:int','str']
    WHEN 'StatusDelta' THEN ARRAY['str','str','str','str','str','str']
    ELSE NULL END;
  IF expected IS NULL OR jsonb_array_length(values_array)<>cardinality(expected) THEN RETURN false; END IF;
  FOR position IN 1..cardinality(expected) LOOP
    item:=values_array->(position-1)->'value';
    IF expected[position] LIKE 'option:%' THEN
      IF item->>'tag'='none' THEN CONTINUE; END IF;
      IF item->>'tag'<>split_part(expected[position],':',2) THEN RETURN false; END IF;
    ELSIF expected[position] LIKE 'tuple:%' THEN
      IF item->>'tag'<>'tuple' OR EXISTS (
        SELECT 1 FROM jsonb_array_elements(item->'children') child
        WHERE CASE WHEN split_part(expected[position],':',2)='str'
                   THEN child->>'tag'<>'str'
                   ELSE child->>'tag'<>'dataclass'
                     OR child->>'name'<>split_part(expected[position],':',2)
                     OR NOT groundloop_m5_matching_validate_logical_dataclass(child)
              END)
      THEN RETURN false; END IF;
    ELSIF item->>'tag'<>expected[position] THEN RETURN false;
    END IF;
  END LOOP;
  IF name_value='RequirementState' AND EXISTS (
    SELECT 1 FROM jsonb_array_elements(values_array->1->'value'->'children') item(value)
    WHERE item.value->>'value'!~'^[0-9a-f]{64}$') THEN RETURN false;
  ELSIF name_value='RequirementState' AND
    (values_array->0->'value'->>'value'='' OR EXISTS (
      SELECT 1 FROM jsonb_array_elements(values_array->2->'value'->'children') item(value)
      WHERE item.value->>'value'='')) THEN RETURN false;
  ELSIF name_value='CombinedClaimState' AND values_array->9->'value'->>'value' NOT IN
    ('supported','unsupported','refuted','conflicted') THEN RETURN false;
  ELSIF name_value='CombinedAnswerState' AND values_array->6->'value'->>'value' NOT IN
    ('valid','partially_supported','unsupported','conflicted','contradicted') THEN RETURN false;
  ELSIF name_value='GroupCertificateRow' AND values_array->2->'value'->>'value'!~'^[0-9a-f]{64}$' THEN RETURN false;
  ELSIF name_value='GroupCertificateRow' AND
    ((values_array->0->'value'->>'value')::integer<0
     OR NOT groundloop_m5_matching_identifier(values_array->1->'value'->>'value')
     OR NOT groundloop_m5_matching_identifier(values_array->3->'value'->>'value')) THEN RETURN false;
  ELSIF name_value='GroupMatchingCertificateArtifact' AND
    (NOT groundloop_m5_matching_identifier(values_array->0->'value'->>'value')
     OR NOT groundloop_m5_matching_identifier(values_array->1->'value'->>'value')
     OR values_array->3->'value'->>'value'<>'m5-group-certificate-v1' OR
     values_array->4->'value'->>'value'!~'^[0-9a-f]{64}$') THEN RETURN false;
  ELSIF name_value='ClaimCertificateArtifact' AND
    (NOT groundloop_m5_matching_identifier(values_array->0->'value'->>'value')
     OR NOT groundloop_m5_matching_identifier(values_array->1->'value'->>'value')
     OR values_array->2->'value'->>'value' NOT IN ('none','direct','group') OR
     EXISTS (SELECT 1 FROM generate_series(3,4) i
       WHERE values_array->i->'value'->>'tag'='str'
         AND NOT groundloop_m5_matching_identifier(values_array->i->'value'->>'value')) OR
     (values_array->5->'value'->>'tag'='str' AND
       values_array->5->'value'->>'value'!~'^[0-9a-f]{64}$') OR
     (values_array->6->'value'->>'tag'='str' AND
       NOT groundloop_m5_matching_identifier(values_array->6->'value'->>'value')) OR
     values_array->7->'value'->>'value'<>'m5-claim-certificate-v2' OR
     values_array->8->'value'->>'value'!~'^[0-9a-f]{64}$') THEN RETURN false;
  ELSIF name_value IN ('WorkingGroupCertificateBinding','WorkingClaimCertificateBinding')
    AND ((values_array->0->'value'->>'value')::bigint<=0
      OR NOT groundloop_m5_matching_identifier(values_array->1->'value'->>'value')
      OR (values_array->2->'value'->>'value')::bigint<=0
      OR (values_array->3->'value'->>'tag'<>'none' AND
          (values_array->3->'value'->>'value')::bigint<=
          (values_array->2->'value'->>'value')::bigint)
      OR values_array->4->'value'->>'value'!~'^[0-9a-f]{64}$') THEN RETURN false;
  ELSIF name_value='StatusDelta' AND
    values_array->1->'value'->>'value' NOT IN ('claim','answer') THEN RETURN false;
  END IF;
  IF name_value='GroupMatchingCertificateArtifact' THEN
    rows_node:=values_array->2->'value';
    IF jsonb_array_length(rows_node->'children') NOT BETWEEN 1 AND 8 THEN RETURN false; END IF;
    digest_fields:=ARRAY['m5-group-certificate-v1','text',values_array->0->'value'->>'value',
      'text',values_array->1->'value'->>'value','int',jsonb_array_length(rows_node->'children')::text,
      'sequence','int',jsonb_array_length(rows_node->'children')::text];
    FOR row_node IN SELECT entry.value FROM jsonb_array_elements(rows_node->'children') entry(value) LOOP
      IF row_node->>'name'<>'GroupCertificateRow'
         OR NOT groundloop_m5_matching_validate_logical_dataclass(row_node)
         OR (row_node->'fields'->0->'value'->>'value')::integer<>row_index
         OR row_node->'fields'->1->'value'->>'value'=ANY(requirement_ids)
         OR row_node->'fields'->2->'value'->>'value'=ANY(text_hashes)
      THEN RETURN false; END IF;
      requirement_ids:=requirement_ids||(row_node->'fields'->1->'value'->>'value');
      text_hashes:=text_hashes||(row_node->'fields'->2->'value'->>'value');
      digest_fields:=digest_fields||ARRAY['sequence','int','4','int',row_index::text,
        'text',row_node->'fields'->1->'value'->>'value','sha256',row_node->'fields'->2->'value'->>'value',
        'text',row_node->'fields'->3->'value'->>'value'];
      row_index:=row_index+1;
    END LOOP;
    expected_digest:=groundloop_m5_matching_digest_text_fields(digest_fields);
    IF values_array->4->'value'->>'value'<>expected_digest THEN RETURN false; END IF;
  ELSIF name_value='ClaimCertificateArtifact' THEN
    support_kind_value:=values_array->2->'value'->>'value';
    IF (support_kind_value='none' AND EXISTS (
          SELECT 1 FROM generate_series(3,5) i WHERE values_array->i->'value'->>'tag'<>'none'))
       OR (support_kind_value='direct' AND (values_array->3->'value'->>'tag'<>'str'
          OR values_array->4->'value'->>'tag'<>'none' OR values_array->5->'value'->>'tag'<>'none'))
       OR (support_kind_value='group' AND (values_array->3->'value'->>'tag'<>'none'
          OR values_array->4->'value'->>'tag'<>'str' OR values_array->5->'value'->>'tag'<>'str'))
    THEN RETURN false; END IF;
    digest_fields:=ARRAY['m5-claim-certificate-v2','text',values_array->0->'value'->>'value',
      'text',values_array->1->'value'->>'value','enum',support_kind_value];
    FOR position IN 3..6 LOOP
      item:=values_array->position->'value';
      digest_fields:=digest_fields||CASE WHEN item->>'tag'='none' THEN ARRAY['null']
        ELSE ARRAY[CASE WHEN position=5 THEN 'sha256' ELSE 'text' END,item->>'value'] END;
    END LOOP;
    expected_digest:=groundloop_m5_matching_digest_text_fields(digest_fields);
    IF values_array->8->'value'->>'value'<>expected_digest THEN RETURN false; END IF;
  END IF;
  RETURN true;
END;
$$;

CREATE FUNCTION groundloop_m5_matching_validate_logical_output(value bytea)
RETURNS jsonb LANGUAGE plpgsql IMMUTABLE STRICT PARALLEL SAFE AS $$
DECLARE root jsonb:=groundloop_m5_matching_parse_logical(value);
DECLARE records jsonb; record_value jsonb; kind_value text; object_id text;
DECLARE after_value jsonb; expected_type text; key_position integer; rank_value integer;
DECLARE prior_rank integer:=-1; output_rows jsonb:='[]'::jsonb;
DECLARE record_index integer:=0; actual_key text; dataclass_valid boolean;
BEGIN
  IF groundloop_m5_matching_reencode_logical(root)<>value THEN
    RAISE EXCEPTION 'logical-output re-encoding mismatch'; END IF;
  IF root->>'tag'<>'tuple' OR jsonb_array_length(root->'children')<>2
     OR root->'children'->0->>'tag'<>'str'
     OR root->'children'->0->>'value'<>'m5-overlay-logical-output-v2'
     OR root->'children'->1->>'tag'<>'tuple'
  THEN RAISE EXCEPTION 'invalid logical-output root'; END IF;
  records:=root->'children'->1->'children';
  FOR record_value IN
    SELECT entry.value FROM jsonb_array_elements(records) AS entry(value)
  LOOP
    record_index:=record_index+1;
    IF record_value->>'tag'<>'tuple' OR jsonb_array_length(record_value->'children')<>3
       OR record_value->'children'->0->>'tag'<>'str'
       OR record_value->'children'->1->>'tag'<>'str'
    THEN RAISE EXCEPTION 'invalid logical-output record'; END IF;
    kind_value:=record_value->'children'->0->>'value';
    object_id:=record_value->'children'->1->>'value';
    after_value:=record_value->'children'->2;
    rank_value:=array_position(ARRAY['requirement_state','group_state','claim_state','answer_state','group_certificate','claim_certificate','group_binding','claim_binding','status_delta'],kind_value);
    IF rank_value IS NULL OR NOT groundloop_m5_matching_identifier(object_id)
       OR rank_value<prior_rank THEN RAISE EXCEPTION 'invalid logical-output relation order'; END IF;
    prior_rank:=rank_value;
    expected_type:=CASE kind_value
      WHEN 'requirement_state' THEN 'RequirementState' WHEN 'group_state' THEN 'GroupState'
      WHEN 'claim_state' THEN 'CombinedClaimState' WHEN 'answer_state' THEN 'CombinedAnswerState'
      WHEN 'group_certificate' THEN 'GroupMatchingCertificateArtifact'
      WHEN 'claim_certificate' THEN 'ClaimCertificateArtifact'
      WHEN 'group_binding' THEN 'WorkingGroupCertificateBinding'
      WHEN 'claim_binding' THEN 'WorkingClaimCertificateBinding' WHEN 'status_delta' THEN 'StatusDelta' END;
    key_position:=CASE kind_value
      WHEN 'group_certificate' THEN 2
      WHEN 'group_binding' THEN 2
      WHEN 'claim_binding' THEN 2
      WHEN 'status_delta' THEN 3
      ELSE 1 END;
    IF after_value->>'tag'='none' THEN
      IF rank_value>6 THEN RAISE EXCEPTION 'logical-output row cannot be absent'; END IF;
    ELSE
      actual_key:=after_value->'fields'->(key_position-1)->'value'->>'value';
      dataclass_valid:=coalesce(groundloop_m5_matching_validate_logical_dataclass(after_value),false);
      IF after_value->>'tag'<>'dataclass' OR after_value->>'name'<>expected_type
         OR NOT dataclass_valid OR actual_key<>object_id
      THEN RAISE EXCEPTION 'logical-output record % type/key mismatch: kind %, expected type %, got type %, expected key %, got key %, dataclass valid %',
        record_index,kind_value,expected_type,after_value->>'name',object_id,
        actual_key,dataclass_valid; END IF;
    END IF;
    IF kind_value='status_delta' AND after_value->'fields'->1->'value'->>'value' NOT IN ('claim','answer') THEN
      RAISE EXCEPTION 'logical-output status object type mismatch';
    END IF;
    output_rows:=output_rows||jsonb_build_array(jsonb_build_object('kind',kind_value,'object_id',object_id,'after',after_value));
  END LOOP;
  RETURN output_rows;
END;
$$;

CREATE FUNCTION groundloop_m5_matching_expect_node(
  node jsonb, expected_tag text, expected_value text DEFAULT NULL
) RETURNS boolean LANGUAGE plpgsql IMMUTABLE PARALLEL SAFE AS $$
BEGIN
  RETURN node->>'tag'=expected_tag AND
    (expected_value IS NULL OR node->>'value'=expected_value);
END;
$$;

CREATE FUNCTION groundloop_m5_matching_json_int_array(node jsonb)
RETURNS bigint[] LANGUAGE plpgsql IMMUTABLE STRICT PARALLEL SAFE AS $$
DECLARE result_value bigint[];
BEGIN
  IF node->>'tag'<>'sequence' OR EXISTS (
    SELECT 1 FROM jsonb_array_elements(node->'children') item(value)
    WHERE item.value->>'tag'<>'int') THEN RAISE EXCEPTION 'expected integer sequence'; END IF;
  SELECT coalesce(array_agg((item.value->>'value')::bigint ORDER BY item.ordinality),ARRAY[]::bigint[])
    INTO result_value FROM jsonb_array_elements(node->'children') WITH ORDINALITY item(value,ordinality);
  RETURN result_value;
END;
$$;

CREATE FUNCTION groundloop_m5_matching_json_text_array(node jsonb)
RETURNS text[] LANGUAGE plpgsql IMMUTABLE STRICT PARALLEL SAFE AS $$
DECLARE result_value text[];
BEGIN
  IF node->>'tag'<>'tuple' OR EXISTS (
    SELECT 1 FROM jsonb_array_elements(node->'children') item(value)
    WHERE item.value->>'tag'<>'str') THEN RAISE EXCEPTION 'expected logical string tuple'; END IF;
  SELECT coalesce(array_agg(item.value->>'value' ORDER BY item.ordinality),ARRAY[]::text[])
    INTO result_value FROM jsonb_array_elements(node->'children') WITH ORDINALITY item(value,ordinality);
  RETURN result_value;
END;
$$;

CREATE FUNCTION groundloop_m5_matching_validate_group_shapes(value bytea)
RETURNS jsonb LANGUAGE plpgsql IMMUTABLE STRICT PARALLEL SAFE AS $$
DECLARE parsed jsonb:=groundloop_m5_matching_parse_typed_preimage(
  value,'m5-persisted-matching-group-shape-set-v1');
DECLARE groups jsonb; group_node jsonb; requirements jsonb; requirement_node jsonb;
DECLARE group_id text; prior_group text:=NULL; requirement_count integer;
DECLARE group_index integer; requirement_index integer; result_value jsonb:='[]'::jsonb;
BEGIN
  IF jsonb_array_length(parsed->'children')<>1 OR parsed->'children'->0->>'tag'<>'sequence' THEN
    RAISE EXCEPTION 'invalid group-shape root';
  END IF;
  groups:=parsed->'children'->0->'children';
  FOR group_index IN 0..jsonb_array_length(groups)-1 LOOP
    group_node:=groups->group_index;
    IF group_node->>'tag'<>'sequence' OR jsonb_array_length(group_node->'children')<>3
       OR group_node->'children'->0->>'tag'<>'text'
       OR group_node->'children'->1->>'tag'<>'int'
       OR group_node->'children'->2->>'tag'<>'sequence'
    THEN RAISE EXCEPTION 'invalid group-shape entry'; END IF;
    group_id:=group_node->'children'->0->>'value';
    requirement_count:=(group_node->'children'->1->>'value')::integer;
    requirements:=group_node->'children'->2->'children';
    IF requirement_count NOT BETWEEN 1 AND 8 OR jsonb_array_length(requirements)<>requirement_count
       OR (prior_group IS NOT NULL AND group_id COLLATE "C"<=prior_group COLLATE "C")
    THEN RAISE EXCEPTION 'group shapes not bounded sorted unique'; END IF;
    FOR requirement_index IN 0..requirement_count-1 LOOP
      requirement_node:=requirements->requirement_index;
      IF requirement_node->>'tag'<>'sequence' OR jsonb_array_length(requirement_node->'children')<>2
         OR NOT groundloop_m5_matching_expect_node(requirement_node->'children'->0,'int',requirement_index::text)
         OR requirement_node->'children'->1->>'tag'<>'text'
      THEN RAISE EXCEPTION 'group-shape requirements not dense'; END IF;
    END LOOP;
    IF NOT groundloop_m5_matching_identifier(group_id) OR EXISTS (
      SELECT 1 FROM jsonb_array_elements(requirements) requirement(value)
      WHERE NOT groundloop_m5_matching_identifier(
        requirement.value->'children'->1->>'value')
    ) THEN RAISE EXCEPTION 'group shape identifiers must be nonempty'; END IF;
    result_value:=result_value||jsonb_build_array(jsonb_build_object(
      'group_id',group_id,'requirement_count',requirement_count,
      'requirements',requirements));
    prior_group:=group_id;
  END LOOP;
  RETURN result_value;
END;
$$;

CREATE FUNCTION groundloop_m5_matching_validate_point(
  option_node jsonb, family text, outer_one text, outer_two text DEFAULT NULL
) RETURNS jsonb LANGUAGE plpgsql IMMUTABLE PARALLEL SAFE AS $$
DECLARE point_node jsonb; values_array jsonb; layer_value text; required_count integer;
DECLARE group_value text; ordinal_value text; payload_present boolean;
DECLARE requirement_value text;
DECLARE epoch_value text; revision_value text;
DECLARE r_value integer; hist bigint[]; neighbours bigint[]; deficiencies_value bigint[];
BEGIN
  IF option_node->>'tag'='null' THEN RETURN NULL; END IF;
  point_node:=option_node;
  IF point_node->>'tag'<>'sequence' THEN RAISE EXCEPTION 'matching point requires SEQ'; END IF;
  values_array:=point_node->'children'; layer_value:=values_array->0->>'value';
  IF values_array->0->>'tag'<>'enum' OR layer_value NOT IN ('current','working') THEN RAISE EXCEPTION 'invalid matching layer'; END IF;
  IF family='observation' THEN
    required_count:=CASE layer_value WHEN 'current' THEN 8 ELSE 9 END;
    IF jsonb_array_length(values_array)<>required_count
       OR (layer_value='current' AND NOT (
         groundloop_m5_matching_expect_node(values_array->1,'text',outer_one) AND
         groundloop_m5_matching_expect_node(values_array->2,'text') AND
         groundloop_m5_matching_expect_node(values_array->3,'text') AND
         groundloop_m5_matching_expect_node(values_array->4,'int') AND
         groundloop_m5_matching_expect_node(values_array->5,'sha256') AND
         groundloop_m5_matching_expect_node(values_array->6,'int') AND groundloop_m5_matching_expect_node(values_array->7,'int')))
       OR (layer_value='working' AND NOT (
         groundloop_m5_matching_expect_node(values_array->1,'int') AND
         groundloop_m5_matching_expect_node(values_array->2,'text',outer_one) AND
         groundloop_m5_matching_expect_node(values_array->3,'text') AND
         groundloop_m5_matching_expect_node(values_array->4,'text') AND
         groundloop_m5_matching_expect_node(values_array->5,'int') AND
         groundloop_m5_matching_expect_node(values_array->6,'sha256') AND
         groundloop_m5_matching_expect_node(values_array->7,'bool') AND groundloop_m5_matching_expect_node(values_array->8,'int')))
    THEN RAISE EXCEPTION 'invalid observation point shape'; END IF;
    group_value:=values_array->(CASE layer_value WHEN 'current' THEN 3 ELSE 4 END)->>'value';
    requirement_value:=values_array->(CASE layer_value WHEN 'current' THEN 2 ELSE 3 END)->>'value';
    ordinal_value:=values_array->(CASE layer_value WHEN 'current' THEN 4 ELSE 5 END)->>'value';
    epoch_value:=values_array->(CASE layer_value WHEN 'current' THEN 6 ELSE 1 END)->>'value';
    revision_value:=values_array->(CASE layer_value WHEN 'current' THEN 7 ELSE 8 END)->>'value';
  ELSIF family='edge' THEN
    required_count:=8;
    IF jsonb_array_length(values_array)<>required_count THEN RAISE EXCEPTION 'invalid edge point arity'; END IF;
    IF layer_value='current' THEN
      IF NOT (groundloop_m5_matching_expect_node(values_array->1,'text',outer_one) AND groundloop_m5_matching_expect_node(values_array->2,'sha256',outer_two) AND groundloop_m5_matching_expect_node(values_array->3,'text') AND groundloop_m5_matching_expect_node(values_array->4,'int') AND groundloop_m5_matching_expect_node(values_array->5,'int') AND (values_array->5->>'value')::bigint>0 AND groundloop_m5_matching_expect_node(values_array->6,'int') AND groundloop_m5_matching_expect_node(values_array->7,'int')) THEN RAISE EXCEPTION 'invalid current edge point'; END IF;
      group_value:=values_array->3->>'value'; ordinal_value:=values_array->4->>'value';
      requirement_value:=outer_one;
      epoch_value:=values_array->6->>'value'; revision_value:=values_array->7->>'value';
    ELSE
      IF NOT (groundloop_m5_matching_expect_node(values_array->1,'int') AND groundloop_m5_matching_expect_node(values_array->2,'text',outer_one) AND groundloop_m5_matching_expect_node(values_array->3,'sha256',outer_two) AND groundloop_m5_matching_expect_node(values_array->4,'text') AND groundloop_m5_matching_expect_node(values_array->5,'int') AND groundloop_m5_matching_expect_node(values_array->6,'int') AND (values_array->6->>'value')::bigint>=0 AND groundloop_m5_matching_expect_node(values_array->7,'int')) THEN RAISE EXCEPTION 'invalid working edge point'; END IF;
      group_value:=values_array->4->>'value'; ordinal_value:=values_array->5->>'value';
      requirement_value:=outer_one;
      epoch_value:=values_array->1->>'value'; revision_value:=values_array->7->>'value';
    END IF;
  ELSIF family='mask' THEN
    required_count:=6;
    IF jsonb_array_length(values_array)<>required_count THEN RAISE EXCEPTION 'invalid mask point arity'; END IF;
    IF layer_value='current' THEN
      IF NOT (groundloop_m5_matching_expect_node(values_array->1,'text',outer_one) AND groundloop_m5_matching_expect_node(values_array->2,'sha256',outer_two) AND groundloop_m5_matching_expect_node(values_array->3,'int') AND (values_array->3->>'value')::bigint>0 AND groundloop_m5_matching_expect_node(values_array->4,'int') AND groundloop_m5_matching_expect_node(values_array->5,'int')) THEN RAISE EXCEPTION 'invalid current mask point'; END IF;
    ELSE
      IF NOT (groundloop_m5_matching_expect_node(values_array->1,'int') AND groundloop_m5_matching_expect_node(values_array->2,'text',outer_one) AND groundloop_m5_matching_expect_node(values_array->3,'sha256',outer_two) AND groundloop_m5_matching_expect_node(values_array->4,'int') AND (values_array->4->>'value')::bigint>=0 AND groundloop_m5_matching_expect_node(values_array->5,'int')) THEN RAISE EXCEPTION 'invalid working mask point'; END IF;
    END IF;
    group_value:=outer_one; ordinal_value:='0';
    epoch_value:=values_array->(CASE layer_value WHEN 'current' THEN 4 ELSE 1 END)->>'value';
    revision_value:=values_array->5->>'value';
  ELSE
    required_count:=CASE layer_value WHEN 'current' THEN 11 ELSE 12 END;
    IF jsonb_array_length(values_array)<>required_count THEN RAISE EXCEPTION 'invalid Hall point arity'; END IF;
    group_value:=values_array->(CASE layer_value WHEN 'current' THEN 1 ELSE 2 END)->>'value'; ordinal_value:='0';
    IF group_value<>outer_one THEN RAISE EXCEPTION 'Hall point repeats different key'; END IF;
    IF layer_value='current' THEN
      IF NOT (groundloop_m5_matching_expect_node(values_array->1,'text',outer_one)
        AND groundloop_m5_matching_expect_node(values_array->2,'int')
        AND (values_array->2->>'value')::integer BETWEEN 1 AND 8
        AND values_array->3->>'tag'='sequence' AND values_array->4->>'tag'='sequence'
        AND values_array->5->>'tag'='sequence'
        AND jsonb_array_length(values_array->3->'children')=(1<<(values_array->2->>'value')::integer)
        AND jsonb_array_length(values_array->4->'children')=(1<<(values_array->2->>'value')::integer)
        AND jsonb_array_length(values_array->5->'children')=(1<<(values_array->2->>'value')::integer)
        AND groundloop_m5_matching_expect_node(values_array->6,'int')
        AND groundloop_m5_matching_expect_node(values_array->7,'int')
        AND groundloop_m5_matching_expect_node(values_array->8,'int')
        AND groundloop_m5_matching_expect_node(values_array->9,'int')
        AND groundloop_m5_matching_expect_node(values_array->10,'int'))
      THEN RAISE EXCEPTION 'invalid current Hall payload'; END IF;
      r_value:=(values_array->2->>'value')::integer;
      hist:=groundloop_m5_matching_json_int_array(values_array->3);
      neighbours:=groundloop_m5_matching_json_int_array(values_array->4);
      deficiencies_value:=groundloop_m5_matching_json_int_array(values_array->5);
      IF NOT groundloop_m5_matching_validate_hall(r_value,hist,neighbours,deficiencies_value,
        (values_array->6->>'value')::integer,(values_array->7->>'value')::integer,
        (values_array->8->>'value')::bigint) THEN RAISE EXCEPTION 'invalid current Hall arithmetic'; END IF;
    ELSE
      payload_present:=values_array->4->>'tag'<>'null';
      IF values_array->3->>'tag'<>'bool' OR EXISTS (
        SELECT 1 FROM generate_series(4,10) index_value
        WHERE (values_array->index_value->>'tag'<>'null')<>payload_present)
      THEN RAISE EXCEPTION 'invalid Hall working payload option shape'; END IF;
      IF (values_array->3->>'value')::boolean<>payload_present THEN RAISE EXCEPTION 'Hall present/payload mismatch'; END IF;
      IF NOT (groundloop_m5_matching_expect_node(values_array->1,'int')
        AND groundloop_m5_matching_expect_node(values_array->2,'text',outer_one)
        AND groundloop_m5_matching_expect_node(values_array->3,'bool')
        AND groundloop_m5_matching_expect_node(values_array->11,'int'))
      THEN RAISE EXCEPTION 'invalid working Hall coordinates'; END IF;
      IF payload_present THEN
        IF values_array->4->>'tag'<>'int'
          OR (values_array->4->>'value')::integer NOT BETWEEN 1 AND 8
          OR values_array->5->>'tag'<>'sequence'
          OR values_array->6->>'tag'<>'sequence'
          OR values_array->7->>'tag'<>'sequence'
          OR jsonb_array_length(values_array->5->'children')<>(1<<(values_array->4->>'value')::integer)
          OR jsonb_array_length(values_array->6->'children')<>(1<<(values_array->4->>'value')::integer)
          OR jsonb_array_length(values_array->7->'children')<>(1<<(values_array->4->>'value')::integer)
          OR EXISTS (SELECT 1 FROM generate_series(8,10) index_value WHERE values_array->index_value->>'tag'<>'int')
        THEN RAISE EXCEPTION 'invalid working Hall payload'; END IF;
        r_value:=(values_array->4->>'value')::integer;
        hist:=groundloop_m5_matching_json_int_array(values_array->5);
        neighbours:=groundloop_m5_matching_json_int_array(values_array->6);
        deficiencies_value:=groundloop_m5_matching_json_int_array(values_array->7);
        IF NOT groundloop_m5_matching_validate_hall(r_value,hist,neighbours,deficiencies_value,
          (values_array->8->>'value')::integer,(values_array->9->>'value')::integer,
          (values_array->10->>'value')::bigint) THEN RAISE EXCEPTION 'invalid working Hall arithmetic'; END IF;
      END IF;
    END IF;
    epoch_value:=values_array->(CASE layer_value WHEN 'current' THEN 9 ELSE 1 END)->>'value';
    revision_value:=values_array->(CASE layer_value WHEN 'current' THEN 10 ELSE 11 END)->>'value';
  END IF;
  IF (layer_value='current' AND
      ((epoch_value)::bigint<=0 OR (revision_value)::bigint<0))
     OR (layer_value='working' AND
      ((epoch_value)::bigint<=0 OR (revision_value)::bigint<=0))
  THEN RAISE EXCEPTION 'matching point coordinates out of range'; END IF;
  IF family IN ('observation','edge') AND
     (NOT groundloop_m5_matching_identifier(requirement_value)
      OR NOT groundloop_m5_matching_identifier(group_value))
     OR family IN ('mask','hall') AND
        NOT groundloop_m5_matching_identifier(group_value)
  THEN RAISE EXCEPTION 'matching point identifier out of domain'; END IF;
  RETURN jsonb_build_object('layer',layer_value,'group',group_value,'ordinal',ordinal_value,
    'requirement',requirement_value,'epoch',epoch_value,'revision',revision_value,'mask',
    CASE WHEN family='mask' THEN values_array->(CASE layer_value WHEN 'current' THEN 3 ELSE 4 END)->>'value' ELSE NULL END,
    'requirement_count',CASE WHEN family='hall' AND
      (layer_value='current' OR payload_present) THEN
      values_array->(CASE layer_value WHEN 'current' THEN 2 ELSE 4 END)->>'value' ELSE NULL END,
    'node',point_node);
END;
$$;

CREATE FUNCTION groundloop_m5_matching_validate_change(value bytea, family text)
RETURNS jsonb LANGUAGE plpgsql IMMUTABLE STRICT PARALLEL SAFE AS $$
DECLARE parsed jsonb; values_array jsonb; before_point jsonb; after_point jsonb;
DECLARE outer_one text; outer_two text; expected_count integer;
BEGIN
  parsed:=groundloop_m5_matching_parse_typed_preimage(value,
    CASE family WHEN 'observation' THEN 'm5-persisted-matching-observation-change-v1'
      WHEN 'edge' THEN 'm5-persisted-matching-edge-change-v1'
      WHEN 'mask' THEN 'm5-persisted-matching-mask-change-v1'
      ELSE 'm5-persisted-matching-hall-change-v1' END);
  values_array:=parsed->'children'; expected_count:=CASE WHEN family IN ('edge','mask') THEN 4 ELSE 3 END;
  IF jsonb_array_length(values_array)<>expected_count OR values_array->0->>'tag'<>'text' THEN RAISE EXCEPTION 'invalid matching change outer shape'; END IF;
  outer_one:=values_array->0->>'value';
  IF NOT groundloop_m5_matching_identifier(outer_one) THEN
    RAISE EXCEPTION 'matching change outer key must be nonempty'; END IF;
  IF family IN ('edge','mask') THEN
    IF values_array->1->>'tag'<>'sha256' THEN RAISE EXCEPTION 'invalid matching change hash key'; END IF;
    outer_two:=values_array->1->>'value';
  END IF;
  before_point:=groundloop_m5_matching_validate_point(values_array->(expected_count-2),family,outer_one,outer_two);
  after_point:=groundloop_m5_matching_validate_point(values_array->(expected_count-1),family,outer_one,outer_two);
  IF before_point IS NULL AND after_point IS NULL THEN RAISE EXCEPTION 'matching change has no point'; END IF;
  IF family='edge' AND before_point IS NOT NULL AND after_point IS NOT NULL
     AND (before_point->>'group',before_point->>'ordinal') IS DISTINCT FROM (after_point->>'group',after_point->>'ordinal')
  THEN RAISE EXCEPTION 'edge change mutates immutable coordinates'; END IF;
  RETURN jsonb_build_object('outer_one',outer_one,'outer_two',outer_two,'before',before_point,'after',after_point,
    'sort_group',coalesce(after_point->>'group',before_point->>'group'),
    'sort_ordinal',coalesce(after_point->>'ordinal',before_point->>'ordinal'),
    'requirement',coalesce(after_point->>'requirement',before_point->>'requirement'));
END;
$$;

CREATE FUNCTION groundloop_m5_matching_validate_logical_patch(
  patch_bytes bytea, output_bytes bytea, expected_epoch bigint,
  expected_revision bigint, decision_policy_version_value text,
  decoded_shapes jsonb DEFAULT NULL
) RETURNS boolean LANGUAGE plpgsql IMMUTABLE PARALLEL SAFE AS $$
DECLARE parsed jsonb:=groundloop_m5_matching_parse_typed_preimage(
  patch_bytes,'m5-persisted-logical-overlay-patch-v1');
DECLARE values_array jsonb:=parsed->'children'; changes jsonb; binding_hashes jsonb;
DECLARE outputs jsonb:=groundloop_m5_matching_validate_logical_output(output_bytes);
DECLARE change_node jsonb; output_node jsonb; children jsonb; kind_value text;
DECLARE object_id text; current_key text; prior_key text:=NULL; change_count integer:=0;
DECLARE prior_kind text; prior_object_id text;
DECLARE state_output_count integer:=0; binding_index integer:=0; binding_value jsonb;
DECLARE fields_value jsonb; binding_kind text; digest_value text; prior_binding_key text:=NULL;
DECLARE is_open boolean; valid_from bigint; valid_to bigint; row_epoch bigint;
DECLARE prior_is_open boolean:=false; prior_binding_kind text; prior_binding_object text;
DECLARE seen_binding_markers jsonb:='[]'::jsonb; marker jsonb; expected_after text;
BEGIN
  IF jsonb_array_length(values_array)<>4 OR values_array->0->>'tag'<>'sequence'
     OR values_array->1->>'tag'<>'sequence' OR values_array->2->>'tag'<>'sha256'
     OR values_array->3->>'tag'<>'int'
     OR values_array->2->>'value'<>encode(public.digest(output_bytes,'sha256'),'hex')
     OR (values_array->3->>'value')::bigint<>octet_length(output_bytes)
  THEN RAISE EXCEPTION 'logical patch output digest or byte count mismatch'; END IF;
  changes:=values_array->0->'children'; binding_hashes:=values_array->1->'children';
  FOR change_node IN SELECT entry.value FROM jsonb_array_elements(changes) entry(value) LOOP
    children:=change_node->'children';
    IF change_node->>'tag'<>'sequence' OR jsonb_array_length(children)<>4
       OR children->0->>'tag'<>'enum' OR children->0->>'value' NOT IN
          ('requirement_state','group_state','claim_state','answer_state','group_certificate','claim_certificate')
       OR children->1->>'tag'<>'text' OR children->2->>'tag' NOT IN ('null','sha256')
       OR children->3->>'tag' NOT IN ('null','sha256')
       OR (children->2->>'tag'='null' AND children->3->>'tag'='null')
    THEN RAISE EXCEPTION 'invalid logical change shape'; END IF;
    kind_value:=children->0->>'value'; object_id:=children->1->>'value';
    IF NOT groundloop_m5_matching_identifier(object_id) THEN
      RAISE EXCEPTION 'logical change object identifier is empty'; END IF;
    IF decoded_shapes IS NOT NULL AND
       kind_value IN ('group_state','group_certificate') AND NOT EXISTS (
      SELECT 1 FROM jsonb_array_elements(decoded_shapes) shape(value)
      WHERE shape.value->>'group_id'=object_id
    ) THEN RAISE EXCEPTION 'logical change lacks covering group shape'; END IF;
    IF prior_kind IS NOT NULL AND ROW(kind_value COLLATE "C",object_id COLLATE "C")
       <= ROW(prior_kind COLLATE "C",prior_object_id COLLATE "C")
    THEN RAISE EXCEPTION 'logical changes not sorted unique'; END IF;
    SELECT output.value INTO output_node FROM jsonb_array_elements(outputs) output(value)
      WHERE output.value->>'kind'=kind_value AND output.value->>'object_id'=object_id;
    IF output_node IS NULL OR ((output_node->'after'->>'tag'='none')=(children->3->>'tag'<>'null')) THEN
      RAISE EXCEPTION 'logical change/output presence mismatch';
    END IF;
    IF children->3->>'tag'<>'null' AND kind_value NOT IN ('group_state','claim_state') THEN
      fields_value:=output_node->'after'->'fields';
      expected_after:=CASE kind_value
        WHEN 'requirement_state' THEN groundloop_m5_runtime_requirement_state_artifact(
          object_id,
          groundloop_m5_matching_json_text_array(fields_value->1->'value'),
          groundloop_m5_matching_json_text_array(fields_value->2->'value'),
          (fields_value->3->'value'->>'value')::integer,
          (fields_value->4->'value'->>'value')::boolean,
          decision_policy_version_value)
        WHEN 'answer_state' THEN groundloop_m5_runtime_answer_state_artifact(
          object_id,(fields_value->1->'value'->>'value')::integer,
          (fields_value->2->'value'->>'value')::integer,
          (fields_value->3->'value'->>'value')::integer,
          (fields_value->4->'value'->>'value')::integer,
          (fields_value->5->'value'->>'value')::integer,
          fields_value->6->'value'->>'value')
        WHEN 'group_certificate' THEN fields_value->4->'value'->>'value'
        WHEN 'claim_certificate' THEN fields_value->8->'value'->>'value'
        ELSE NULL END;
      IF children->3->>'value'<>expected_after THEN
        RAISE EXCEPTION 'logical change derivable after hash mismatch'; END IF;
    END IF;
    prior_kind:=kind_value; prior_object_id:=object_id; change_count:=change_count+1;
  END LOOP;
  SELECT count(*) INTO state_output_count FROM jsonb_array_elements(outputs) output(value)
    WHERE output.value->>'kind' IN ('requirement_state','group_state','claim_state','answer_state','group_certificate','claim_certificate');
  IF state_output_count<>change_count THEN RAISE EXCEPTION 'logical change/output multiset mismatch'; END IF;
  FOR output_node IN SELECT entry.value FROM jsonb_array_elements(outputs) entry(value)
    WHERE entry.value->>'kind' IN ('group_binding','claim_binding')
  LOOP
    binding_index:=binding_index+1; binding_value:=output_node->'after'; fields_value:=binding_value->'fields';
    binding_kind:=CASE output_node->>'kind' WHEN 'group_binding' THEN 'group' ELSE 'claim' END;
    row_epoch:=(fields_value->0->'value'->>'value')::bigint;
    valid_from:=(fields_value->2->'value'->>'value')::bigint;
    is_open:=fields_value->3->'value'->>'tag'='none';
    valid_to:=CASE WHEN is_open THEN NULL ELSE (fields_value->3->'value'->>'value')::bigint END;
    IF row_epoch<>expected_epoch OR (is_open AND valid_from<>expected_revision)
       OR (NOT is_open AND valid_to<>expected_revision) THEN RAISE EXCEPTION 'logical binding point mismatch'; END IF;
    marker:=jsonb_build_array(binding_kind,output_node->>'object_id',is_open);
    IF seen_binding_markers @> jsonb_build_array(marker)
       OR (prior_binding_kind=binding_kind AND prior_binding_object=output_node->>'object_id'
           AND (prior_is_open OR NOT is_open))
       OR (is_open AND seen_binding_markers @> jsonb_build_array(
             jsonb_build_array(binding_kind,output_node->>'object_id',false))
           AND (prior_binding_kind,prior_binding_object) IS DISTINCT FROM
               (binding_kind,output_node->>'object_id'))
    THEN RAISE EXCEPTION 'binding close/open order mismatch'; END IF;
    IF binding_index>jsonb_array_length(binding_hashes) OR binding_hashes->(binding_index-1)->>'tag'<>'sha256' THEN RAISE EXCEPTION 'binding digest sequence mismatch'; END IF;
    digest_value:=groundloop_m5_matching_digest_text_fields(ARRAY[
      'm5-persisted-certificate-binding-row-v1','enum',binding_kind,
      'int',row_epoch::text,'text',output_node->>'object_id','int',valid_from::text]
      || CASE WHEN is_open THEN ARRAY['null'] ELSE ARRAY['int',valid_to::text] END
      || ARRAY['sha256',fields_value->4->'value'->>'value']);
    IF digest_value<>binding_hashes->(binding_index-1)->>'value' THEN RAISE EXCEPTION 'binding digest/output mismatch'; END IF;
    seen_binding_markers:=seen_binding_markers||jsonb_build_array(marker);
    prior_binding_kind:=binding_kind; prior_binding_object:=output_node->>'object_id'; prior_is_open:=is_open;
  END LOOP;
  IF binding_index<>jsonb_array_length(binding_hashes) THEN RAISE EXCEPTION 'binding digest/output cardinality mismatch'; END IF;
  RETURN true;
END;
$$;

CREATE FUNCTION groundloop_m5_matching_validate_patch_change_point(
  decoded_change jsonb, source_kind_value text, before_epoch bigint,
  before_revision bigint, resulting_epoch bigint, resulting_revision bigint
) RETURNS boolean LANGUAGE plpgsql IMMUTABLE STRICT PARALLEL SAFE AS $$
DECLARE before_point jsonb:=decoded_change->'before'; after_point jsonb:=decoded_change->'after';
BEGIN
  IF after_point IS NULL OR after_point='null'::jsonb
     OR after_point->>'layer'<>'working'
     OR (after_point->>'epoch')::bigint<>resulting_epoch
     OR (after_point->>'revision')::bigint<>resulting_revision
  THEN RAISE EXCEPTION 'physical after point disagrees with patch result'; END IF;
  IF before_point IS NOT NULL AND before_point<>'null'::jsonb THEN
    IF source_kind_value='structural_open' AND before_point->>'layer'<>'current' THEN
      RAISE EXCEPTION 'structural-open before point must be current';
    ELSIF source_kind_value<>'structural_open' AND before_point->>'layer'='working'
      AND ((before_point->>'epoch')::bigint<>resulting_epoch
        OR (before_point->>'revision')::bigint>before_revision)
    THEN RAISE EXCEPTION 'later working before point disagrees with patch'; END IF;
  END IF;
  IF (source_kind_value='structural_open' AND
      (resulting_revision<>1 OR before_epoch=resulting_epoch))
     OR (source_kind_value<>'structural_open' AND
      (before_epoch<>resulting_epoch OR resulting_revision<>before_revision+1))
  THEN RAISE EXCEPTION 'outer patch point law mismatch'; END IF;
  RETURN true;
END;
$$;

CREATE FUNCTION groundloop_m5_matching_validate_change_shape(
  decoded_change jsonb, decoded_shapes jsonb, family text
) RETURNS boolean LANGUAGE plpgsql IMMUTABLE STRICT PARALLEL SAFE AS $$
BEGIN
  IF family IN ('observation','edge') AND NOT EXISTS (
    SELECT 1 FROM jsonb_array_elements(decoded_shapes) shape(value)
    WHERE shape.value->>'group_id'=decoded_change->>'sort_group'
      AND (decoded_change->>'sort_ordinal')::integer>=0
      AND (decoded_change->>'sort_ordinal')::integer<
          (shape.value->>'requirement_count')::integer
      AND shape.value->'requirements'->(decoded_change->>'sort_ordinal')::integer
            ->'children'->1->>'value'=decoded_change->>'requirement')
  THEN RAISE EXCEPTION '% change lacks covering group shape',family;
  ELSIF family='mask' AND NOT EXISTS (
    SELECT 1 FROM jsonb_array_elements(decoded_shapes) shape(value)
    WHERE shape.value->>'group_id'=decoded_change->>'outer_one'
      AND (decoded_change->'before'='null'::jsonb OR
           (decoded_change->'before'->>'mask')::integer BETWEEN 0 AND
             (1 << (shape.value->>'requirement_count')::integer)-1)
      AND (decoded_change->'after'='null'::jsonb OR
           (decoded_change->'after'->>'mask')::integer BETWEEN 0 AND
             (1 << (shape.value->>'requirement_count')::integer)-1)
  ) THEN RAISE EXCEPTION 'mask change lacks covering group shape';
  ELSIF family='hall' AND NOT EXISTS (
    SELECT 1 FROM jsonb_array_elements(decoded_shapes) shape(value)
    WHERE shape.value->>'group_id'=decoded_change->>'outer_one'
      AND (decoded_change->'before'='null'::jsonb OR
           decoded_change->'before'->>'requirement_count' IS NULL OR
           (decoded_change->'before'->>'requirement_count')::integer=
             (shape.value->>'requirement_count')::integer)
      AND (decoded_change->'after'='null'::jsonb OR
           decoded_change->'after'->>'requirement_count' IS NULL OR
           (decoded_change->'after'->>'requirement_count')::integer=
             (shape.value->>'requirement_count')::integer)
  ) THEN RAISE EXCEPTION 'Hall change lacks covering group shape';
  END IF;
  RETURN true;
END;
$$;

CREATE FUNCTION groundloop_m5_matching_validate_change_order(
  previous_change jsonb, current_change jsonb, family text
) RETURNS boolean LANGUAGE plpgsql IMMUTABLE PARALLEL SAFE AS $$
BEGIN
  IF previous_change IS NULL THEN RETURN true; END IF;
  IF family IN ('observation','hall') AND
     (current_change->>'outer_one') COLLATE "C" <=
     (previous_change->>'outer_one') COLLATE "C"
  THEN RAISE EXCEPTION '% changes not sorted unique',family;
  ELSIF family='edge' AND
    ROW((current_change->>'sort_group') COLLATE "C",
        (current_change->>'sort_ordinal')::integer,
        (current_change->>'outer_two') COLLATE "C",
        (current_change->>'outer_one') COLLATE "C") <=
    ROW((previous_change->>'sort_group') COLLATE "C",
        (previous_change->>'sort_ordinal')::integer,
        (previous_change->>'outer_two') COLLATE "C",
        (previous_change->>'outer_one') COLLATE "C")
  THEN RAISE EXCEPTION 'edge changes not sorted unique';
  ELSIF family='mask' AND
    ROW((current_change->>'outer_one') COLLATE "C",
        (current_change->>'outer_two') COLLATE "C") <=
    ROW((previous_change->>'outer_one') COLLATE "C",
        (previous_change->>'outer_two') COLLATE "C")
  THEN RAISE EXCEPTION 'mask changes not sorted unique';
  END IF;
  RETURN true;
END;
$$;

CREATE FUNCTION groundloop_m5_matching_work_values(row_to_hash anyelement)
RETURNS bigint[] LANGUAGE plpgsql IMMUTABLE STRICT PARALLEL SAFE AS $$
DECLARE value_json jsonb := to_jsonb(row_to_hash);
BEGIN
  RETURN ARRAY[
    (value_json->>'contribution_additions')::bigint,
    (value_json->>'contribution_removals')::bigint,
    (value_json->>'requirement_observation_changes_processed')::bigint,
    (value_json->>'policy_candidate_observations')::bigint,
    (value_json->>'ordered_policy_range_probes')::bigint,
    (value_json->>'ordered_index_operations')::bigint,
    (value_json->>'canonical_sort_items')::bigint,
    (value_json->>'edge_refcount_keys_updated')::bigint,
    (value_json->>'distinct_edge_crossings')::bigint,
    (value_json->>'hash_mask_transitions')::bigint,
    (value_json->>'hash_masks_initialized')::bigint,
    (value_json->>'hall_zeta_additions')::bigint,
    (value_json->>'hall_subset_entries_examined')::bigint,
    (value_json->>'hall_neighbor_entries_changed')::bigint,
    (value_json->>'hall_deficiency_entries_examined')::bigint,
    (value_json->>'certificate_repairs')::bigint,
    (value_json->>'certificate_reconstructions')::bigint,
    (value_json->>'policy_rebindings')::bigint,
    (value_json->>'representative_hashes_read')::bigint,
    (value_json->>'representative_observations_read')::bigint,
    (value_json->>'augmenting_searches')::bigint,
    (value_json->>'augmenting_requirement_visits')::bigint,
    (value_json->>'augmenting_edge_visits')::bigint,
    (value_json->>'certificate_digest_input_bytes')::bigint,
    (value_json->>'group_local_state_operations')::bigint,
    (value_json->>'groups_touched')::bigint,
    (value_json->>'claims_touched')::bigint,
    (value_json->>'answers_touched')::bigint,
    (value_json->>'claim_status_changes')::bigint,
    (value_json->>'answer_status_changes')::bigint,
    (value_json->>'output_bytes')::bigint,
    (value_json->>'requirement_state_only_changes')::bigint,
    (value_json->>'group_state_only_changes')::bigint,
    (value_json->>'claim_state_only_changes')::bigint,
    (value_json->>'group_certificate_only_changes')::bigint,
    (value_json->>'claim_certificate_only_changes')::bigint,
    (value_json->>'public_status_deltas')::bigint
  ];
END;
$$;

CREATE FUNCTION groundloop_m5_matching_work_digest(values_to_hash bigint[])
RETURNS char(64) LANGUAGE plpgsql IMMUTABLE STRICT PARALLEL SAFE AS $$
DECLARE fields text[] := ARRAY['m5-matching-work-v1']; item bigint;
BEGIN
  IF cardinality(values_to_hash)<>37
     OR EXISTS (SELECT 1 FROM unnest(values_to_hash) value WHERE value<0)
  THEN RAISE EXCEPTION 'matching work requires 37 nonnegative counters'; END IF;
  FOREACH item IN ARRAY values_to_hash LOOP
    fields := fields || ARRAY['int',item::text];
  END LOOP;
  RETURN groundloop_m5_matching_digest_text_fields(fields);
END;
$$;

CREATE FUNCTION groundloop_m5_matching_validate_hall(
    requirement_count integer,
    mask_histogram bigint[],
    neighbor_counts bigint[],
    deficiencies bigint[],
    maximum_deficiency integer,
    matching_size integer,
    distinct_hash_count bigint
) RETURNS boolean
LANGUAGE plpgsql IMMUTABLE STRICT PARALLEL SAFE AS $$
DECLARE
    expected_size integer := (1 << requirement_count);
    subset integer;
    mask integer;
    expected_neighbor bigint;
    expected_max integer := 0;
BEGIN
    IF requirement_count NOT BETWEEN 1 AND 8
       OR cardinality(mask_histogram) <> expected_size
       OR cardinality(neighbor_counts) <> expected_size
       OR cardinality(deficiencies) <> expected_size
       OR mask_histogram[1] <> 0 OR neighbor_counts[1] <> 0
       OR deficiencies[1] <> 0
       OR EXISTS (SELECT 1 FROM unnest(mask_histogram) value WHERE value < 0)
       OR EXISTS (SELECT 1 FROM unnest(neighbor_counts) value WHERE value < 0)
    THEN RETURN false; END IF;
    FOR subset IN 1..expected_size - 1 LOOP
        expected_neighbor := 0;
        FOR mask IN 1..expected_size - 1 LOOP
            IF (mask & subset) <> 0 THEN
                expected_neighbor := expected_neighbor + mask_histogram[mask + 1];
            END IF;
        END LOOP;
        IF neighbor_counts[subset + 1] <> expected_neighbor
           OR deficiencies[subset + 1] <>
              bit_count(subset::bit(32))::integer - expected_neighbor
        THEN RETURN false; END IF;
        expected_max := greatest(expected_max, deficiencies[subset + 1]);
    END LOOP;
    RETURN maximum_deficiency = expected_max
       AND matching_size = requirement_count - expected_max
       AND distinct_hash_count = (
           SELECT coalesce(sum(value), 0) FROM unnest(mask_histogram) value
       );
END;
$$;

-- groundloop:m5-persisted-matching-group:image_schema
CREATE TABLE groundloop_m5_matching_image_current (
    singleton boolean PRIMARY KEY CHECK (singleton),
    decision_policy_version text NOT NULL REFERENCES groundloop_decision_policy(policy_version),
    installed_epoch_id bigint NOT NULL REFERENCES groundloop_epoch(epoch_id),
    installed_revision bigint NOT NULL CHECK (installed_revision >= 0)
);
CREATE TABLE groundloop_m5_matching_image_working (
    epoch_id bigint PRIMARY KEY REFERENCES groundloop_m5_runtime_epoch(epoch_id),
    base_epoch_id bigint NOT NULL REFERENCES groundloop_epoch(epoch_id),
    base_revision bigint NOT NULL CHECK (base_revision >= 0),
    decision_policy_version text NOT NULL REFERENCES groundloop_decision_policy(policy_version),
    updated_revision bigint NOT NULL CHECK (updated_revision >= 1)
);

-- groundloop:m5-persisted-matching-group:observation_schema
CREATE TABLE groundloop_m5_matching_observation_current (
    observation_id text PRIMARY KEY REFERENCES groundloop_semantic_observation(observation_id),
    requirement_version_id text NOT NULL REFERENCES groundloop_m5_requirement_version(requirement_version_id),
    group_version_id text NOT NULL REFERENCES groundloop_m5_group_version(group_version_id),
    requirement_ordinal integer NOT NULL CHECK (requirement_ordinal >= 0),
    text_hash char(64) NOT NULL CHECK (groundloop_m5_matching_sha256(text_hash)),
    installed_epoch_id bigint NOT NULL REFERENCES groundloop_epoch(epoch_id),
    installed_revision bigint NOT NULL CHECK (installed_revision >= 0)
);
CREATE INDEX groundloop_m5_matching_observation_current_representative
    ON groundloop_m5_matching_observation_current(
        group_version_id, requirement_ordinal,
        text_hash COLLATE "C", observation_id COLLATE "C"
    );
CREATE TABLE groundloop_m5_matching_observation_working (
    epoch_id bigint NOT NULL REFERENCES groundloop_m5_runtime_epoch(epoch_id),
    observation_id text NOT NULL REFERENCES groundloop_semantic_observation(observation_id),
    requirement_version_id text NOT NULL REFERENCES groundloop_m5_requirement_version(requirement_version_id),
    group_version_id text NOT NULL REFERENCES groundloop_m5_group_version(group_version_id),
    requirement_ordinal integer NOT NULL CHECK (requirement_ordinal >= 0),
    text_hash char(64) NOT NULL CHECK (groundloop_m5_matching_sha256(text_hash)),
    present boolean NOT NULL,
    updated_revision bigint NOT NULL CHECK (updated_revision >= 1),
    PRIMARY KEY(epoch_id, observation_id)
);
CREATE INDEX groundloop_m5_matching_observation_working_representative
    ON groundloop_m5_matching_observation_working(
        epoch_id, group_version_id, requirement_ordinal,
        text_hash COLLATE "C", observation_id COLLATE "C"
    ) WHERE present;

-- groundloop:m5-persisted-matching-group:edge_mask_schema
CREATE TABLE groundloop_m5_matching_edge_current (
    requirement_version_id text NOT NULL REFERENCES groundloop_m5_requirement_version(requirement_version_id),
    text_hash char(64) NOT NULL CHECK (groundloop_m5_matching_sha256(text_hash)),
    group_version_id text NOT NULL REFERENCES groundloop_m5_group_version(group_version_id),
    requirement_ordinal integer NOT NULL CHECK (requirement_ordinal >= 0),
    refcount bigint NOT NULL CHECK (refcount > 0),
    installed_epoch_id bigint NOT NULL REFERENCES groundloop_epoch(epoch_id),
    installed_revision bigint NOT NULL CHECK (installed_revision >= 0),
    PRIMARY KEY(requirement_version_id, text_hash)
);
CREATE INDEX groundloop_m5_matching_edge_current_group
    ON groundloop_m5_matching_edge_current(group_version_id, requirement_ordinal, text_hash COLLATE "C");
CREATE TABLE groundloop_m5_matching_edge_working (
    epoch_id bigint NOT NULL REFERENCES groundloop_m5_runtime_epoch(epoch_id),
    requirement_version_id text NOT NULL REFERENCES groundloop_m5_requirement_version(requirement_version_id),
    text_hash char(64) NOT NULL CHECK (groundloop_m5_matching_sha256(text_hash)),
    group_version_id text NOT NULL REFERENCES groundloop_m5_group_version(group_version_id),
    requirement_ordinal integer NOT NULL CHECK (requirement_ordinal >= 0),
    refcount bigint NOT NULL CHECK (refcount >= 0),
    updated_revision bigint NOT NULL CHECK (updated_revision >= 1),
    PRIMARY KEY(epoch_id, requirement_version_id, text_hash)
);
CREATE INDEX groundloop_m5_matching_edge_working_group
    ON groundloop_m5_matching_edge_working(epoch_id, group_version_id, requirement_ordinal, text_hash COLLATE "C");
CREATE TABLE groundloop_m5_matching_hash_mask_current (
    group_version_id text NOT NULL REFERENCES groundloop_m5_group_version(group_version_id),
    text_hash char(64) NOT NULL CHECK (groundloop_m5_matching_sha256(text_hash)),
    mask integer NOT NULL CHECK (mask > 0 AND mask <= 255),
    installed_epoch_id bigint NOT NULL REFERENCES groundloop_epoch(epoch_id),
    installed_revision bigint NOT NULL CHECK (installed_revision >= 0),
    PRIMARY KEY(group_version_id, text_hash)
);
CREATE INDEX groundloop_m5_matching_hash_mask_current_representative
    ON groundloop_m5_matching_hash_mask_current(group_version_id, mask, text_hash COLLATE "C");
CREATE TABLE groundloop_m5_matching_hash_mask_working (
    epoch_id bigint NOT NULL REFERENCES groundloop_m5_runtime_epoch(epoch_id),
    group_version_id text NOT NULL REFERENCES groundloop_m5_group_version(group_version_id),
    text_hash char(64) NOT NULL CHECK (groundloop_m5_matching_sha256(text_hash)),
    mask integer NOT NULL CHECK (mask BETWEEN 0 AND 255),
    updated_revision bigint NOT NULL CHECK (updated_revision >= 1),
    PRIMARY KEY(epoch_id, group_version_id, text_hash)
);
CREATE INDEX groundloop_m5_matching_hash_mask_working_representative
    ON groundloop_m5_matching_hash_mask_working(epoch_id, group_version_id, mask, text_hash COLLATE "C")
    WHERE mask > 0;

-- groundloop:m5-persisted-matching-group:hall_schema
CREATE TABLE groundloop_m5_matching_hall_current (
    group_version_id text PRIMARY KEY REFERENCES groundloop_m5_group_version(group_version_id),
    requirement_count integer NOT NULL,
    mask_histogram bigint[] NOT NULL,
    neighbor_counts bigint[] NOT NULL,
    deficiencies bigint[] NOT NULL,
    maximum_deficiency integer NOT NULL,
    matching_size integer NOT NULL,
    distinct_hash_count bigint NOT NULL,
    installed_epoch_id bigint NOT NULL REFERENCES groundloop_epoch(epoch_id),
    installed_revision bigint NOT NULL CHECK (installed_revision >= 0),
    CHECK (groundloop_m5_matching_validate_hall(requirement_count, mask_histogram,
        neighbor_counts, deficiencies, maximum_deficiency, matching_size,
        distinct_hash_count))
);
CREATE TABLE groundloop_m5_matching_hall_working (
    epoch_id bigint NOT NULL REFERENCES groundloop_m5_runtime_epoch(epoch_id),
    group_version_id text NOT NULL REFERENCES groundloop_m5_group_version(group_version_id),
    present boolean NOT NULL,
    requirement_count integer,
    mask_histogram bigint[], neighbor_counts bigint[], deficiencies bigint[],
    maximum_deficiency integer, matching_size integer, distinct_hash_count bigint,
    updated_revision bigint NOT NULL CHECK (updated_revision >= 1),
    PRIMARY KEY(epoch_id, group_version_id),
    CHECK ((NOT present AND requirement_count IS NULL AND mask_histogram IS NULL
        AND neighbor_counts IS NULL AND deficiencies IS NULL
        AND maximum_deficiency IS NULL AND matching_size IS NULL
        AND distinct_hash_count IS NULL) OR
      (present AND groundloop_m5_matching_validate_hall(requirement_count,
        mask_histogram, neighbor_counts, deficiencies, maximum_deficiency,
        matching_size, distinct_hash_count)))
);

-- groundloop:m5-persisted-matching-group:artifact_schema
CREATE TABLE groundloop_m5_matching_patch_artifact (
    patch_digest char(64) PRIMARY KEY CHECK (groundloop_m5_matching_sha256(patch_digest)),
    source_kind text NOT NULL CHECK (source_kind IN ('structural_open','requirement_completion','direct_transition')),
    source_id text NOT NULL CHECK (btrim(source_id) <> ''),
    source_identity_hash char(64) NOT NULL CHECK (groundloop_m5_matching_sha256(source_identity_hash)),
    before_epoch_id bigint NOT NULL REFERENCES groundloop_epoch(epoch_id),
    before_revision bigint NOT NULL CHECK (before_revision >= 0),
    resulting_epoch_id bigint NOT NULL REFERENCES groundloop_epoch(epoch_id),
    resulting_revision bigint NOT NULL CHECK (resulting_revision >= 1),
    decision_policy_version text NOT NULL REFERENCES groundloop_decision_policy(policy_version),
    group_shape_set_digest char(64) NOT NULL CHECK (groundloop_m5_matching_sha256(group_shape_set_digest)),
    group_shape_set_preimage bytea NOT NULL,
    observation_change_digests char(64)[] NOT NULL,
    observation_change_preimages bytea[] NOT NULL,
    edge_change_digests char(64)[] NOT NULL, edge_change_preimages bytea[] NOT NULL,
    mask_change_digests char(64)[] NOT NULL, mask_change_preimages bytea[] NOT NULL,
    hall_change_digests char(64)[] NOT NULL, hall_change_preimages bytea[] NOT NULL,
    logical_overlay_patch_digest char(64) NOT NULL CHECK (groundloop_m5_matching_sha256(logical_overlay_patch_digest)),
    logical_overlay_patch_preimage bytea NOT NULL,
    logical_output_preimage bytea NOT NULL,
    matching_work_digest char(64) NOT NULL CHECK (groundloop_m5_matching_sha256(matching_work_digest)),
    canonical_patch_preimage bytea NOT NULL,
    CHECK (cardinality(observation_change_digests)=cardinality(observation_change_preimages)),
    CHECK (cardinality(edge_change_digests)=cardinality(edge_change_preimages)),
    CHECK (cardinality(mask_change_digests)=cardinality(mask_change_preimages)),
    CHECK (cardinality(hall_change_digests)=cardinality(hall_change_preimages)),
    CHECK ((source_kind='structural_open' AND resulting_revision=1)
        OR (source_kind<>'structural_open' AND before_epoch_id=resulting_epoch_id
            AND resulting_revision=before_revision+1))
);

CREATE TABLE groundloop_m5_matching_work_accumulator (
    epoch_id bigint PRIMARY KEY REFERENCES groundloop_m5_runtime_epoch(epoch_id),
    contribution_additions bigint NOT NULL, contribution_removals bigint NOT NULL,
    requirement_observation_changes_processed bigint NOT NULL,
    policy_candidate_observations bigint NOT NULL, ordered_policy_range_probes bigint NOT NULL,
    ordered_index_operations bigint NOT NULL, canonical_sort_items bigint NOT NULL,
    edge_refcount_keys_updated bigint NOT NULL, distinct_edge_crossings bigint NOT NULL,
    hash_mask_transitions bigint NOT NULL, hash_masks_initialized bigint NOT NULL,
    hall_zeta_additions bigint NOT NULL, hall_subset_entries_examined bigint NOT NULL,
    hall_neighbor_entries_changed bigint NOT NULL, hall_deficiency_entries_examined bigint NOT NULL,
    certificate_repairs bigint NOT NULL, certificate_reconstructions bigint NOT NULL,
    policy_rebindings bigint NOT NULL, representative_hashes_read bigint NOT NULL,
    representative_observations_read bigint NOT NULL, augmenting_searches bigint NOT NULL,
    augmenting_requirement_visits bigint NOT NULL, augmenting_edge_visits bigint NOT NULL,
    certificate_digest_input_bytes bigint NOT NULL, group_local_state_operations bigint NOT NULL,
    groups_touched bigint NOT NULL, claims_touched bigint NOT NULL, answers_touched bigint NOT NULL,
    claim_status_changes bigint NOT NULL, answer_status_changes bigint NOT NULL, output_bytes bigint NOT NULL,
    requirement_state_only_changes bigint NOT NULL, group_state_only_changes bigint NOT NULL,
    claim_state_only_changes bigint NOT NULL, group_certificate_only_changes bigint NOT NULL,
    claim_certificate_only_changes bigint NOT NULL, public_status_deltas bigint NOT NULL,
    matching_work_digest char(64) NOT NULL CHECK (groundloop_m5_matching_sha256(matching_work_digest)),
    updated_revision bigint NOT NULL CHECK (updated_revision >= 1),
    CHECK (least(contribution_additions, contribution_removals,
      requirement_observation_changes_processed, policy_candidate_observations,
      ordered_policy_range_probes, ordered_index_operations, canonical_sort_items,
      edge_refcount_keys_updated, distinct_edge_crossings, hash_mask_transitions,
      hash_masks_initialized, hall_zeta_additions, hall_subset_entries_examined,
      hall_neighbor_entries_changed, hall_deficiency_entries_examined,
      certificate_repairs, certificate_reconstructions, policy_rebindings,
      representative_hashes_read, representative_observations_read,
      augmenting_searches, augmenting_requirement_visits, augmenting_edge_visits,
      certificate_digest_input_bytes, group_local_state_operations, groups_touched,
      claims_touched, answers_touched, claim_status_changes, answer_status_changes,
      output_bytes, requirement_state_only_changes, group_state_only_changes,
      claim_state_only_changes, group_certificate_only_changes,
      claim_certificate_only_changes, public_status_deltas) >= 0)
);

CREATE TABLE groundloop_m5_matching_work_contribution (
    epoch_id bigint NOT NULL REFERENCES groundloop_m5_runtime_epoch(epoch_id),
    source_kind text NOT NULL CHECK (source_kind IN ('structural_open','requirement_completion','direct_transition')),
    source_id text NOT NULL CHECK (btrim(source_id) <> ''),
    source_identity_hash char(64) NOT NULL CHECK (groundloop_m5_matching_sha256(source_identity_hash)),
    before_epoch_id bigint NOT NULL REFERENCES groundloop_epoch(epoch_id),
    before_revision bigint NOT NULL CHECK (before_revision >= 0),
    resulting_revision bigint NOT NULL CHECK (resulting_revision >= 1),
    patch_digest char(64) NOT NULL REFERENCES groundloop_m5_matching_patch_artifact(patch_digest),
    contribution_additions bigint NOT NULL, contribution_removals bigint NOT NULL,
    requirement_observation_changes_processed bigint NOT NULL,
    policy_candidate_observations bigint NOT NULL, ordered_policy_range_probes bigint NOT NULL,
    ordered_index_operations bigint NOT NULL, canonical_sort_items bigint NOT NULL,
    edge_refcount_keys_updated bigint NOT NULL, distinct_edge_crossings bigint NOT NULL,
    hash_mask_transitions bigint NOT NULL, hash_masks_initialized bigint NOT NULL,
    hall_zeta_additions bigint NOT NULL, hall_subset_entries_examined bigint NOT NULL,
    hall_neighbor_entries_changed bigint NOT NULL, hall_deficiency_entries_examined bigint NOT NULL,
    certificate_repairs bigint NOT NULL, certificate_reconstructions bigint NOT NULL,
    policy_rebindings bigint NOT NULL, representative_hashes_read bigint NOT NULL,
    representative_observations_read bigint NOT NULL, augmenting_searches bigint NOT NULL,
    augmenting_requirement_visits bigint NOT NULL, augmenting_edge_visits bigint NOT NULL,
    certificate_digest_input_bytes bigint NOT NULL, group_local_state_operations bigint NOT NULL,
    groups_touched bigint NOT NULL, claims_touched bigint NOT NULL, answers_touched bigint NOT NULL,
    claim_status_changes bigint NOT NULL, answer_status_changes bigint NOT NULL, output_bytes bigint NOT NULL,
    requirement_state_only_changes bigint NOT NULL, group_state_only_changes bigint NOT NULL,
    claim_state_only_changes bigint NOT NULL, group_certificate_only_changes bigint NOT NULL,
    claim_certificate_only_changes bigint NOT NULL, public_status_deltas bigint NOT NULL,
    matching_work_digest char(64) NOT NULL CHECK (groundloop_m5_matching_sha256(matching_work_digest)),
    contribution_digest char(64) NOT NULL UNIQUE CHECK (groundloop_m5_matching_sha256(contribution_digest)),
    PRIMARY KEY(epoch_id, source_kind, source_id), UNIQUE(epoch_id, resulting_revision),
    CHECK (least(contribution_additions, contribution_removals,
      requirement_observation_changes_processed, policy_candidate_observations,
      ordered_policy_range_probes, ordered_index_operations, canonical_sort_items,
      edge_refcount_keys_updated, distinct_edge_crossings, hash_mask_transitions,
      hash_masks_initialized, hall_zeta_additions, hall_subset_entries_examined,
      hall_neighbor_entries_changed, hall_deficiency_entries_examined,
      certificate_repairs, certificate_reconstructions, policy_rebindings,
      representative_hashes_read, representative_observations_read,
      augmenting_searches, augmenting_requirement_visits, augmenting_edge_visits,
      certificate_digest_input_bytes, group_local_state_operations, groups_touched,
      claims_touched, answers_touched, claim_status_changes, answer_status_changes,
      output_bytes, requirement_state_only_changes, group_state_only_changes,
      claim_state_only_changes, group_certificate_only_changes,
      claim_certificate_only_changes, public_status_deltas) >= 0)
);

CREATE FUNCTION groundloop_m5_matching_validate_patch_artifact()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
  position integer;
  fields text[];
  child bytea;
  child_digest text;
  expected_outer text[];
  decoded_change jsonb;
  prior_key text := NULL;
  current_key text;
  decoded_shapes jsonb;
  shape_row jsonb;
  prior_outer_one text;
  prior_outer_two text;
  prior_group text;
  prior_ordinal integer;
  prior_change jsonb;
BEGIN
  IF (NEW.source_kind='structural_open' AND
      (NEW.resulting_revision<>1 OR NEW.before_epoch_id=NEW.resulting_epoch_id))
     OR (NEW.source_kind<>'structural_open' AND
      (NEW.before_epoch_id<>NEW.resulting_epoch_id OR
       NEW.resulting_revision<>NEW.before_revision+1))
  THEN RAISE EXCEPTION 'outer patch point law mismatch'; END IF;
  IF groundloop_m5_matching_hash_preimage(NEW.group_shape_set_preimage) <>
     NEW.group_shape_set_digest THEN
    RAISE EXCEPTION 'persisted matching group-shape preimage mismatch';
  END IF;
  fields := groundloop_m5_matching_decode_preimage(NEW.group_shape_set_preimage);
  PERFORM groundloop_m5_matching_parse_typed_preimage(
    NEW.group_shape_set_preimage,'m5-persisted-matching-group-shape-set-v1');
  decoded_shapes:=groundloop_m5_matching_validate_group_shapes(NEW.group_shape_set_preimage);
  IF fields[1] <> 'm5-persisted-matching-group-shape-set-v1' THEN
    RAISE EXCEPTION 'persisted matching group-shape domain mismatch';
  END IF;
  FOR position IN 1..cardinality(NEW.observation_change_preimages) LOOP
    child := NEW.observation_change_preimages[position];
    child_digest := NEW.observation_change_digests[position];
    fields := groundloop_m5_matching_decode_preimage(child);
    PERFORM groundloop_m5_matching_parse_typed_preimage(
      child,'m5-persisted-matching-observation-change-v1');
    decoded_change:=groundloop_m5_matching_validate_change(child,'observation');
    PERFORM groundloop_m5_matching_validate_patch_change_point(decoded_change,NEW.source_kind,NEW.before_epoch_id,NEW.before_revision,NEW.resulting_epoch_id,NEW.resulting_revision);
    PERFORM groundloop_m5_matching_validate_change_shape(decoded_change,decoded_shapes,'observation');
    PERFORM groundloop_m5_matching_validate_change_order(prior_change,decoded_change,'observation');
    prior_change:=decoded_change;
    IF fields[1] <> 'm5-persisted-matching-observation-change-v1'
       OR groundloop_m5_matching_hash_preimage(child)<>child_digest
    THEN RAISE EXCEPTION 'persisted matching observation child mismatch'; END IF;
  END LOOP;
  prior_change:=NULL;
  FOR position IN 1..cardinality(NEW.edge_change_preimages) LOOP
    child := NEW.edge_change_preimages[position];
    child_digest := NEW.edge_change_digests[position];
    fields := groundloop_m5_matching_decode_preimage(child);
    PERFORM groundloop_m5_matching_parse_typed_preimage(
      child,'m5-persisted-matching-edge-change-v1');
    decoded_change:=groundloop_m5_matching_validate_change(child,'edge');
    PERFORM groundloop_m5_matching_validate_patch_change_point(decoded_change,NEW.source_kind,NEW.before_epoch_id,NEW.before_revision,NEW.resulting_epoch_id,NEW.resulting_revision);
    PERFORM groundloop_m5_matching_validate_change_shape(decoded_change,decoded_shapes,'edge');
    PERFORM groundloop_m5_matching_validate_change_order(prior_change,decoded_change,'edge');
    prior_change:=decoded_change;
    IF fields[1] <> 'm5-persisted-matching-edge-change-v1'
       OR groundloop_m5_matching_hash_preimage(child)<>child_digest
    THEN RAISE EXCEPTION 'persisted matching edge child mismatch'; END IF;
  END LOOP;
  prior_change:=NULL;
  FOR position IN 1..cardinality(NEW.mask_change_preimages) LOOP
    child := NEW.mask_change_preimages[position];
    child_digest := NEW.mask_change_digests[position];
    fields := groundloop_m5_matching_decode_preimage(child);
    PERFORM groundloop_m5_matching_parse_typed_preimage(
      child,'m5-persisted-matching-mask-change-v1');
    decoded_change:=groundloop_m5_matching_validate_change(child,'mask');
    PERFORM groundloop_m5_matching_validate_patch_change_point(decoded_change,NEW.source_kind,NEW.before_epoch_id,NEW.before_revision,NEW.resulting_epoch_id,NEW.resulting_revision);
    PERFORM groundloop_m5_matching_validate_change_shape(decoded_change,decoded_shapes,'mask');
    PERFORM groundloop_m5_matching_validate_change_order(prior_change,decoded_change,'mask');
    prior_change:=decoded_change;
    IF fields[1] <> 'm5-persisted-matching-mask-change-v1'
       OR groundloop_m5_matching_hash_preimage(child)<>child_digest
    THEN RAISE EXCEPTION 'persisted matching mask child mismatch'; END IF;
  END LOOP;
  prior_change:=NULL;
  FOR position IN 1..cardinality(NEW.hall_change_preimages) LOOP
    child := NEW.hall_change_preimages[position];
    child_digest := NEW.hall_change_digests[position];
    fields := groundloop_m5_matching_decode_preimage(child);
    PERFORM groundloop_m5_matching_parse_typed_preimage(
      child,'m5-persisted-matching-hall-change-v1');
    decoded_change:=groundloop_m5_matching_validate_change(child,'hall');
    PERFORM groundloop_m5_matching_validate_patch_change_point(decoded_change,NEW.source_kind,NEW.before_epoch_id,NEW.before_revision,NEW.resulting_epoch_id,NEW.resulting_revision);
    PERFORM groundloop_m5_matching_validate_change_shape(decoded_change,decoded_shapes,'hall');
    PERFORM groundloop_m5_matching_validate_change_order(prior_change,decoded_change,'hall');
    prior_change:=decoded_change;
    IF fields[1] <> 'm5-persisted-matching-hall-change-v1'
       OR groundloop_m5_matching_hash_preimage(child)<>child_digest
    THEN RAISE EXCEPTION 'persisted matching Hall child mismatch'; END IF;
  END LOOP;
  IF groundloop_m5_matching_hash_preimage(NEW.logical_overlay_patch_preimage)<>
       NEW.logical_overlay_patch_digest
     OR (groundloop_m5_matching_decode_preimage(
          NEW.logical_overlay_patch_preimage))[1] <>
       'm5-persisted-logical-overlay-patch-v1'
  THEN RAISE EXCEPTION 'persisted matching logical patch preimage mismatch'; END IF;
  PERFORM groundloop_m5_matching_parse_typed_preimage(
    NEW.logical_overlay_patch_preimage,'m5-persisted-logical-overlay-patch-v1');
  PERFORM groundloop_m5_matching_validate_logical_output(NEW.logical_output_preimage);
  PERFORM groundloop_m5_matching_validate_logical_patch(
    NEW.logical_overlay_patch_preimage,NEW.logical_output_preimage,
    NEW.resulting_epoch_id,NEW.resulting_revision,NEW.decision_policy_version,
    decoded_shapes);
  expected_outer := ARRAY[
    'm5-persisted-matching-patch-v1', 'enum', NEW.source_kind,
    'text', NEW.source_id, 'sha256', NEW.source_identity_hash,
    'int', NEW.before_epoch_id::text, 'int', NEW.before_revision::text,
    'int', NEW.resulting_epoch_id::text, 'int', NEW.resulting_revision::text,
    'text', NEW.decision_policy_version, 'sha256', NEW.group_shape_set_digest
  ];
  expected_outer := expected_outer || ARRAY['sequence','int',cardinality(NEW.observation_change_digests)::text];
  FOREACH child_digest IN ARRAY NEW.observation_change_digests LOOP expected_outer:=expected_outer||ARRAY['sha256',child_digest]; END LOOP;
  expected_outer := expected_outer || ARRAY['sequence','int',cardinality(NEW.edge_change_digests)::text];
  FOREACH child_digest IN ARRAY NEW.edge_change_digests LOOP expected_outer:=expected_outer||ARRAY['sha256',child_digest]; END LOOP;
  expected_outer := expected_outer || ARRAY['sequence','int',cardinality(NEW.mask_change_digests)::text];
  FOREACH child_digest IN ARRAY NEW.mask_change_digests LOOP expected_outer:=expected_outer||ARRAY['sha256',child_digest]; END LOOP;
  expected_outer := expected_outer || ARRAY['sequence','int',cardinality(NEW.hall_change_digests)::text];
  FOREACH child_digest IN ARRAY NEW.hall_change_digests LOOP expected_outer:=expected_outer||ARRAY['sha256',child_digest]; END LOOP;
  expected_outer := expected_outer || ARRAY['sha256',NEW.logical_overlay_patch_digest,'sha256',NEW.matching_work_digest];
  IF groundloop_m5_matching_decode_preimage(NEW.canonical_patch_preimage)<>
       expected_outer OR groundloop_m5_matching_digest_text_fields(expected_outer)<>NEW.patch_digest
  THEN RAISE EXCEPTION 'persisted matching canonical patch preimage mismatch'; END IF;
  PERFORM groundloop_m5_matching_parse_typed_preimage(
    NEW.canonical_patch_preimage,'m5-persisted-matching-patch-v1');
  RETURN NEW;
END;
$$;
CREATE TRIGGER groundloop_m5_matching_patch_artifact_bytes
BEFORE INSERT OR UPDATE ON groundloop_m5_matching_patch_artifact
FOR EACH ROW EXECUTE FUNCTION groundloop_m5_matching_validate_patch_artifact();

CREATE FUNCTION groundloop_m5_matching_validate_work_row()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE values_to_hash bigint[] := groundloop_m5_matching_work_values(NEW);
DECLARE expected_contribution char(64);
BEGIN
  IF groundloop_m5_matching_work_digest(values_to_hash)<>NEW.matching_work_digest
  THEN RAISE EXCEPTION 'persisted matching work digest mismatch'; END IF;
  IF TG_TABLE_NAME='groundloop_m5_matching_work_contribution' THEN
    expected_contribution := groundloop_m5_matching_digest_text_fields(ARRAY[
      'm5-matching-work-contribution-v1','int',NEW.epoch_id::text,
      'enum',NEW.source_kind,'text',NEW.source_id,
      'sha256',NEW.source_identity_hash,'int',NEW.before_epoch_id::text,
      'int',NEW.before_revision::text,'int',NEW.resulting_revision::text,
      'sha256',NEW.patch_digest,'sha256',NEW.matching_work_digest]);
    IF expected_contribution<>NEW.contribution_digest THEN
      RAISE EXCEPTION 'persisted matching contribution digest mismatch';
    END IF;
    IF (NEW.source_kind='structural_open' AND
        (NEW.resulting_revision<>1 OR NEW.before_epoch_id=NEW.epoch_id))
       OR (NEW.source_kind<>'structural_open' AND
           (NEW.before_epoch_id<>NEW.epoch_id OR
            NEW.resulting_revision<>NEW.before_revision+1))
    THEN RAISE EXCEPTION 'persisted matching contribution point law mismatch'; END IF;
  END IF;
  RETURN NEW;
END;
$$;
CREATE TRIGGER groundloop_m5_matching_contribution_bytes
BEFORE INSERT OR UPDATE ON groundloop_m5_matching_work_contribution
FOR EACH ROW EXECUTE FUNCTION groundloop_m5_matching_validate_work_row();
CREATE TRIGGER groundloop_m5_matching_accumulator_bytes
BEFORE INSERT OR UPDATE ON groundloop_m5_matching_work_accumulator
FOR EACH ROW EXECUTE FUNCTION groundloop_m5_matching_validate_work_row();

-- groundloop:m5-persisted-matching-group:authorization
CREATE FUNCTION groundloop_m5_matching_journal_key(
    relation_value text, parts text[]
) RETURNS bytea LANGUAGE plpgsql IMMUTABLE STRICT PARALLEL SAFE AS $$
DECLARE payload bytea := ''::bytea;
DECLARE part text;
BEGIN
  FOREACH part IN ARRAY ARRAY[relation_value] || parts LOOP
    payload := payload || int8send(octet_length(convert_to(part,'UTF8')))
      || convert_to(part,'UTF8');
  END LOOP;
  RETURN payload;
END;
$$;

CREATE FUNCTION groundloop_m5_matching_row_key(relation_value text,row_value jsonb)
RETURNS bytea LANGUAGE plpgsql IMMUTABLE STRICT PARALLEL SAFE AS $$
DECLARE key_parts text[];
BEGIN
  key_parts:=CASE relation_value
    WHEN 'groundloop_m5_matching_image_current' THEN ARRAY[row_value->>'singleton']
    WHEN 'groundloop_m5_matching_observation_current' THEN ARRAY[row_value->>'observation_id']
    WHEN 'groundloop_m5_matching_edge_current' THEN ARRAY[row_value->>'requirement_version_id',row_value->>'text_hash']
    WHEN 'groundloop_m5_matching_hash_mask_current' THEN ARRAY[row_value->>'group_version_id',row_value->>'text_hash']
    WHEN 'groundloop_m5_matching_hall_current' THEN ARRAY[row_value->>'group_version_id']
    WHEN 'groundloop_m5_matching_image_working' THEN ARRAY[row_value->>'epoch_id']
    WHEN 'groundloop_m5_matching_observation_working' THEN ARRAY[row_value->>'epoch_id',row_value->>'observation_id']
    WHEN 'groundloop_m5_matching_edge_working' THEN ARRAY[row_value->>'epoch_id',row_value->>'requirement_version_id',row_value->>'text_hash']
    WHEN 'groundloop_m5_matching_hash_mask_working' THEN ARRAY[row_value->>'epoch_id',row_value->>'group_version_id',row_value->>'text_hash']
    WHEN 'groundloop_m5_matching_hall_working' THEN ARRAY[row_value->>'epoch_id',row_value->>'group_version_id']
    WHEN 'groundloop_m5_matching_patch_artifact' THEN ARRAY[row_value->>'patch_digest']
    WHEN 'groundloop_m5_matching_work_contribution' THEN ARRAY[row_value->>'epoch_id',row_value->>'source_kind',row_value->>'source_id',row_value->>'resulting_revision']
    WHEN 'groundloop_m5_matching_work_accumulator' THEN ARRAY[row_value->>'epoch_id']
    WHEN 'groundloop_m5_working_requirement_state' THEN ARRAY[row_value->>'epoch_id',row_value->>'requirement_version_id']
    WHEN 'groundloop_m5_working_group_state' THEN ARRAY[row_value->>'epoch_id',row_value->>'group_version_id']
    WHEN 'groundloop_m5_working_claim_state' THEN ARRAY[row_value->>'epoch_id',row_value->>'claim_id']
    WHEN 'groundloop_m5_working_answer_state' THEN ARRAY[row_value->>'epoch_id',row_value->>'answer_version_id']
    WHEN 'groundloop_m5_group_certificate_artifact' THEN ARRAY[row_value->>'certificate_digest']
    WHEN 'groundloop_m5_group_certificate_artifact_row' THEN ARRAY[row_value->>'certificate_digest',row_value->>'requirement_ordinal']
    WHEN 'groundloop_m5_claim_certificate_artifact' THEN ARRAY[row_value->>'certificate_digest']
    WHEN 'groundloop_m5_working_group_certificate_binding' THEN ARRAY[row_value->>'epoch_id',row_value->>'group_version_id',row_value->>'valid_from_revision']
    WHEN 'groundloop_m5_working_claim_certificate_binding' THEN ARRAY[row_value->>'epoch_id',row_value->>'claim_id',row_value->>'valid_from_revision']
    ELSE NULL END;
  IF key_parts IS NULL OR array_position(key_parts,NULL) IS NOT NULL THEN
    RAISE EXCEPTION 'unsupported or incomplete persisted matching journal key for %',relation_value;
  END IF;
  RETURN groundloop_m5_matching_journal_key(relation_value,key_parts);
END;
$$;

CREATE FUNCTION groundloop_m5_matching_encode_fields(fields_value text[])
RETURNS bytea LANGUAGE plpgsql IMMUTABLE STRICT PARALLEL SAFE AS $$
DECLARE result_value bytea:=''::bytea;
DECLARE field_value text;
DECLARE encoded_value bytea;
BEGIN
  FOREACH field_value IN ARRAY fields_value LOOP
    IF field_value IS NULL THEN RAISE EXCEPTION 'canonical journal field is NULL'; END IF;
    encoded_value:=convert_to(field_value,'UTF8');
    result_value:=result_value||int8send(octet_length(encoded_value))||encoded_value;
  END LOOP;
  RETURN result_value;
END;
$$;

CREATE FUNCTION groundloop_m5_matching_typed_fields(node jsonb)
RETURNS text[] LANGUAGE plpgsql IMMUTABLE STRICT PARALLEL SAFE AS $$
DECLARE tag_value text:=node->>'tag';
DECLARE result_value text[];
DECLARE child_value jsonb;
BEGIN
  IF tag_value='null' THEN RETURN ARRAY['null']; END IF;
  IF tag_value IN ('text','enum','sha256','int','bool','f64') THEN
    RETURN ARRAY[tag_value,CASE WHEN tag_value='bool' THEN
      CASE WHEN (node->>'value')::boolean THEN '1' ELSE '0' END
      ELSE node->>'value' END];
  END IF;
  IF tag_value<>'sequence' THEN RAISE EXCEPTION 'unsupported typed journal node'; END IF;
  result_value:=ARRAY['sequence','int',jsonb_array_length(node->'children')::text];
  FOR child_value IN SELECT entry.value FROM jsonb_array_elements(node->'children') entry(value)
  LOOP result_value:=result_value||groundloop_m5_matching_typed_fields(child_value); END LOOP;
  RETURN result_value;
END;
$$;

CREATE FUNCTION groundloop_m5_matching_logical_scalar(tag_value text,value_value text)
RETURNS bytea LANGUAGE plpgsql IMMUTABLE PARALLEL SAFE AS $$
DECLARE payload bytea;
BEGIN
  IF tag_value='none' THEN RETURN convert_to('n','UTF8'); END IF;
  IF tag_value='bool' THEN RETURN convert_to('b','UTF8')||decode(
    CASE WHEN value_value='true' THEN '01' ELSE '00' END,'hex'); END IF;
  IF tag_value='f64' THEN RETURN convert_to('f','UTF8')||decode(value_value,'hex'); END IF;
  payload:=convert_to(value_value,'UTF8');
  RETURN convert_to(CASE tag_value WHEN 'enum' THEN 'e' WHEN 'str' THEN 's' ELSE 'i' END,'UTF8')
    ||int8send(octet_length(payload))||payload;
END;
$$;

CREATE FUNCTION groundloop_m5_matching_logical_tuple(values_value bytea[])
RETURNS bytea LANGUAGE plpgsql IMMUTABLE STRICT PARALLEL SAFE AS $$
DECLARE result_value bytea:=convert_to('q','UTF8')||int8send(cardinality(values_value));
DECLARE value_value bytea;
BEGIN
  FOREACH value_value IN ARRAY values_value LOOP
    result_value:=result_value||int8send(octet_length(value_value))||value_value;
  END LOOP;
  RETURN result_value;
END;
$$;

CREATE FUNCTION groundloop_m5_matching_logical_dataclass(
  name_value text,names_value text[],values_value bytea[]
) RETURNS bytea LANGUAGE plpgsql IMMUTABLE STRICT PARALLEL SAFE AS $$
DECLARE name_bytes bytea:=convert_to(name_value,'UTF8');
DECLARE field_bytes bytea;
DECLARE result_value bytea:=convert_to('d','UTF8')||int8send(octet_length(name_bytes))
  ||name_bytes||int8send(cardinality(names_value));
DECLARE position_value integer;
BEGIN
  IF cardinality(names_value)<>cardinality(values_value) THEN
    RAISE EXCEPTION 'logical journal dataclass arity mismatch'; END IF;
  FOR position_value IN 1..cardinality(names_value) LOOP
    name_bytes:=convert_to(names_value[position_value],'UTF8');
    field_bytes:=values_value[position_value];
    result_value:=result_value||int8send(octet_length(name_bytes))||name_bytes
      ||int8send(octet_length(field_bytes))||field_bytes;
  END LOOP;
  RETURN result_value;
END;
$$;

CREATE FUNCTION groundloop_m5_matching_logical_text_tuple(values_value jsonb)
RETURNS bytea LANGUAGE plpgsql IMMUTABLE STRICT PARALLEL SAFE AS $$
DECLARE children bytea[]:=ARRAY[]::bytea[];
DECLARE child_value jsonb;
BEGIN
  FOR child_value IN SELECT entry.value FROM jsonb_array_elements(values_value) entry(value)
  LOOP children:=children||groundloop_m5_matching_logical_scalar('str',child_value#>>'{}'); END LOOP;
  RETURN groundloop_m5_matching_logical_tuple(children);
END;
$$;

CREATE FUNCTION groundloop_m5_matching_logical_row_preimage(
  kind_value text,row_value jsonb
) RETURNS bytea LANGUAGE plpgsql IMMUTABLE STRICT PARALLEL SAFE AS $$
DECLARE names_value text[];
DECLARE values_value bytea[];
DECLARE score_value text;
BEGIN
  IF kind_value='requirement_state' THEN
    names_value:=ARRAY['requirement_version_id','witness_hashes','supporting_observation_ids','witness_count','satisfied'];
    values_value:=ARRAY[
      groundloop_m5_matching_logical_scalar('str',row_value->>'requirement_version_id'),
      groundloop_m5_matching_logical_text_tuple(row_value->'witness_hashes'),
      groundloop_m5_matching_logical_text_tuple(row_value->'supporting_observation_ids'),
      groundloop_m5_matching_logical_scalar('int',row_value->>'witness_count'),
      groundloop_m5_matching_logical_scalar('bool',row_value->>'satisfied')];
    RETURN groundloop_m5_matching_logical_dataclass('RequirementState',names_value,values_value);
  ELSIF kind_value='group_state' THEN
    names_value:=ARRAY['group_version_id','requirement_count','satisfied_count','matching_size','complete'];
    values_value:=ARRAY[
      groundloop_m5_matching_logical_scalar('str',row_value->>'group_version_id'),
      groundloop_m5_matching_logical_scalar('int',row_value->>'requirement_count'),
      groundloop_m5_matching_logical_scalar('int',row_value->>'satisfied_count'),
      groundloop_m5_matching_logical_scalar('int',row_value->>'matching_size'),
      groundloop_m5_matching_logical_scalar('bool',row_value->>'complete')];
    RETURN groundloop_m5_matching_logical_dataclass('GroupState',names_value,values_value);
  ELSIF kind_value='claim_state' THEN
    names_value:=ARRAY['claim_id','support_count','refute_count','best_support_score','best_refute_score','supporting_observation_ids','refuting_observation_ids','complete_group_count','complete_group_ids','status'];
    values_value:=ARRAY[
      groundloop_m5_matching_logical_scalar('str',row_value->>'claim_id'),
      groundloop_m5_matching_logical_scalar('int',row_value->>'support_count'),
      groundloop_m5_matching_logical_scalar('int',row_value->>'refute_count'),
      CASE WHEN row_value->'best_support_score'='null'::jsonb THEN groundloop_m5_matching_logical_scalar('none',NULL)
        ELSE groundloop_m5_matching_logical_scalar('f64',encode(float8send((row_value->>'best_support_score')::double precision),'hex')) END,
      CASE WHEN row_value->'best_refute_score'='null'::jsonb THEN groundloop_m5_matching_logical_scalar('none',NULL)
        ELSE groundloop_m5_matching_logical_scalar('f64',encode(float8send((row_value->>'best_refute_score')::double precision),'hex')) END,
      groundloop_m5_matching_logical_text_tuple(row_value->'supporting_observation_ids'),
      groundloop_m5_matching_logical_text_tuple(row_value->'refuting_observation_ids'),
      groundloop_m5_matching_logical_scalar('int',row_value->>'complete_group_count'),
      groundloop_m5_matching_logical_text_tuple(row_value->'complete_group_ids'),
      groundloop_m5_matching_logical_scalar('enum',row_value->>'status')];
    RETURN groundloop_m5_matching_logical_dataclass('CombinedClaimState',names_value,values_value);
  ELSIF kind_value='answer_state' THEN
    names_value:=ARRAY['answer_version_id','required_claim_count','supported_count','unsupported_count','refuted_count','conflicted_count','status'];
    values_value:=ARRAY[
      groundloop_m5_matching_logical_scalar('str',row_value->>'answer_version_id'),
      groundloop_m5_matching_logical_scalar('int',row_value->>'required_claim_count'),
      groundloop_m5_matching_logical_scalar('int',row_value->>'supported_count'),
      groundloop_m5_matching_logical_scalar('int',row_value->>'unsupported_count'),
      groundloop_m5_matching_logical_scalar('int',row_value->>'refuted_count'),
      groundloop_m5_matching_logical_scalar('int',row_value->>'conflicted_count'),
      groundloop_m5_matching_logical_scalar('enum',row_value->>'status')];
    RETURN groundloop_m5_matching_logical_dataclass('CombinedAnswerState',names_value,values_value);
  ELSIF kind_value IN ('group_binding','claim_binding') THEN
    names_value:=CASE kind_value WHEN 'group_binding' THEN
      ARRAY['epoch_id','group_version_id','valid_from_revision','valid_to_revision','certificate_digest']
      ELSE ARRAY['epoch_id','claim_id','valid_from_revision','valid_to_revision','certificate_digest'] END;
    values_value:=ARRAY[
      groundloop_m5_matching_logical_scalar('int',row_value->>'epoch_id'),
      groundloop_m5_matching_logical_scalar('str',CASE kind_value WHEN 'group_binding' THEN row_value->>'group_version_id' ELSE row_value->>'claim_id' END),
      groundloop_m5_matching_logical_scalar('int',row_value->>'valid_from_revision'),
      CASE WHEN row_value->'valid_to_revision'='null'::jsonb THEN groundloop_m5_matching_logical_scalar('none',NULL)
        ELSE groundloop_m5_matching_logical_scalar('int',row_value->>'valid_to_revision') END,
      groundloop_m5_matching_logical_scalar('str',row_value->>'certificate_digest')];
    RETURN groundloop_m5_matching_logical_dataclass(
      CASE kind_value WHEN 'group_binding' THEN 'WorkingGroupCertificateBinding' ELSE 'WorkingClaimCertificateBinding' END,
      names_value,values_value);
  END IF;
  RAISE EXCEPTION 'unsupported logical journal row %',kind_value;
END;
$$;

CREATE FUNCTION groundloop_m5_matching_raw_text_array(value jsonb)
RETURNS text[] LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE AS $$
  SELECT coalesce(array_agg(entry.value ORDER BY entry.ordinality),ARRAY[]::text[])
  FROM jsonb_array_elements_text(value) WITH ORDINALITY entry(value,ordinality)
$$;

CREATE FUNCTION groundloop_m5_matching_state_digest(kind_value text,row_value jsonb)
RETURNS char(64) LANGUAGE plpgsql IMMUTABLE STRICT PARALLEL SAFE AS $$
DECLARE fields text[];
DECLARE item text;
BEGIN
  IF kind_value='requirement_state' THEN
    fields:=ARRAY['m5-requirement-state-artifact-v2','text',
      row_value->>'requirement_version_id','sequence','int',
      jsonb_array_length(row_value->'witness_hashes')::text];
    FOR item IN SELECT value FROM jsonb_array_elements_text(row_value->'witness_hashes')
    LOOP fields:=fields||ARRAY['sha256',item]; END LOOP;
    fields:=fields||ARRAY['sequence','int',
      jsonb_array_length(row_value->'supporting_observation_ids')::text];
    FOR item IN SELECT value FROM jsonb_array_elements_text(
      row_value->'supporting_observation_ids')
    LOOP fields:=fields||ARRAY['text',item]; END LOOP;
    fields:=fields||ARRAY['int',row_value->>'witness_count','bool',
      CASE WHEN (row_value->>'satisfied')::boolean THEN '1' ELSE '0' END,
      'text',row_value->>'decision_policy_version'];
  ELSIF kind_value='group_state' THEN
    fields:=ARRAY['m5-group-state-artifact-v2','text',row_value->>'group_version_id',
      'int',row_value->>'requirement_count','int',row_value->>'satisfied_count',
      'int',row_value->>'matching_size','bool',
      CASE WHEN (row_value->>'complete')::boolean THEN '1' ELSE '0' END,
      'text',row_value->>'decision_policy_version']||CASE
        WHEN row_value->'certificate_digest'='null'::jsonb THEN ARRAY['null']
        ELSE ARRAY['sha256',row_value->>'certificate_digest'] END;
  ELSIF kind_value='claim_state' THEN
    fields:=ARRAY['m5-claim-state-artifact-v2','text',row_value->>'claim_id',
      'int',row_value->>'support_count','int',row_value->>'refute_count'];
    fields:=fields||CASE WHEN row_value->'best_support_score'='null'::jsonb
      THEN ARRAY['null'] ELSE ARRAY['f64',encode(float8send(
        (row_value->>'best_support_score')::double precision),'hex')] END;
    fields:=fields||CASE WHEN row_value->'best_refute_score'='null'::jsonb
      THEN ARRAY['null'] ELSE ARRAY['f64',encode(float8send(
        (row_value->>'best_refute_score')::double precision),'hex')] END;
    fields:=fields||ARRAY['sequence','int',
      jsonb_array_length(row_value->'supporting_observation_ids')::text];
    FOR item IN SELECT value FROM jsonb_array_elements_text(
      row_value->'supporting_observation_ids')
    LOOP fields:=fields||ARRAY['text',item]; END LOOP;
    fields:=fields||ARRAY['sequence','int',
      jsonb_array_length(row_value->'refuting_observation_ids')::text];
    FOR item IN SELECT value FROM jsonb_array_elements_text(
      row_value->'refuting_observation_ids')
    LOOP fields:=fields||ARRAY['text',item]; END LOOP;
    fields:=fields||ARRAY['int',row_value->>'complete_group_count','sequence','int',
      jsonb_array_length(row_value->'complete_group_ids')::text];
    FOR item IN SELECT value FROM jsonb_array_elements_text(
      row_value->'complete_group_ids')
    LOOP fields:=fields||ARRAY['text',item]; END LOOP;
    fields:=fields||ARRAY['enum',row_value->>'status','text',
      row_value->>'decision_policy_version','sha256',row_value->>'certificate_digest'];
  ELSIF kind_value='answer_state' THEN
    fields:=ARRAY['m5-answer-state-artifact-v2','text',
      row_value->>'answer_version_id','int',row_value->>'required_claim_count',
      'int',row_value->>'supported_count','int',row_value->>'unsupported_count',
      'int',row_value->>'refuted_count','int',row_value->>'conflicted_count',
      'enum',row_value->>'status'];
  ELSE RAISE EXCEPTION 'unsupported persisted matching state digest kind'; END IF;
  RETURN groundloop_m5_matching_digest_text_fields(fields);
END;
$$;

CREATE FUNCTION groundloop_m5_matching_certificate_preimage(
  kind_value text,digest_value char(64)
) RETURNS bytea LANGUAGE plpgsql STABLE STRICT PARALLEL SAFE AS $$
DECLARE artifact_value record;
DECLARE row_value record;
DECLARE row_preimages bytea[]:=ARRAY[]::bytea[];
DECLARE values_value bytea[];
DECLARE names_value text[];
BEGIN
  IF kind_value='group_certificate' THEN
    SELECT * INTO STRICT artifact_value FROM groundloop_m5_group_certificate_artifact
      WHERE certificate_digest=digest_value;
    FOR row_value IN SELECT * FROM groundloop_m5_group_certificate_artifact_row
      WHERE certificate_digest=digest_value ORDER BY requirement_ordinal
    LOOP
      row_preimages:=row_preimages||groundloop_m5_matching_logical_dataclass(
        'GroupCertificateRow',
        ARRAY['requirement_ordinal','requirement_version_id','text_hash','selected_observation_id'],
        ARRAY[
          groundloop_m5_matching_logical_scalar('int',row_value.requirement_ordinal::text),
          groundloop_m5_matching_logical_scalar('str',row_value.requirement_version_id),
          groundloop_m5_matching_logical_scalar('str',row_value.text_hash::text),
          groundloop_m5_matching_logical_scalar('str',row_value.selected_observation_id)]);
    END LOOP;
    names_value:=ARRAY['decision_policy_version','group_version_id','rows','certificate_version','certificate_digest'];
    values_value:=ARRAY[
      groundloop_m5_matching_logical_scalar('str',artifact_value.decision_policy_version),
      groundloop_m5_matching_logical_scalar('str',artifact_value.group_version_id),
      groundloop_m5_matching_logical_tuple(row_preimages),
      groundloop_m5_matching_logical_scalar('str',artifact_value.certificate_version),
      groundloop_m5_matching_logical_scalar('str',artifact_value.certificate_digest::text)];
    RETURN groundloop_m5_matching_logical_dataclass(
      'GroupMatchingCertificateArtifact',names_value,values_value);
  ELSIF kind_value='claim_certificate' THEN
    SELECT * INTO STRICT artifact_value FROM groundloop_m5_claim_certificate_artifact
      WHERE certificate_digest=digest_value;
    names_value:=ARRAY['claim_id','decision_policy_version','support_kind',
      'direct_support_observation_id','group_version_id','group_certificate_digest',
      'direct_refute_observation_id','certificate_version','certificate_digest'];
    values_value:=ARRAY[
      groundloop_m5_matching_logical_scalar('str',artifact_value.claim_id),
      groundloop_m5_matching_logical_scalar('str',artifact_value.decision_policy_version),
      groundloop_m5_matching_logical_scalar('enum',artifact_value.support_kind),
      CASE WHEN artifact_value.direct_support_observation_id IS NULL THEN groundloop_m5_matching_logical_scalar('none',NULL)
        ELSE groundloop_m5_matching_logical_scalar('str',artifact_value.direct_support_observation_id) END,
      CASE WHEN artifact_value.group_version_id IS NULL THEN groundloop_m5_matching_logical_scalar('none',NULL)
        ELSE groundloop_m5_matching_logical_scalar('str',artifact_value.group_version_id) END,
      CASE WHEN artifact_value.group_certificate_digest IS NULL THEN groundloop_m5_matching_logical_scalar('none',NULL)
        ELSE groundloop_m5_matching_logical_scalar('str',artifact_value.group_certificate_digest::text) END,
      CASE WHEN artifact_value.direct_refute_observation_id IS NULL THEN groundloop_m5_matching_logical_scalar('none',NULL)
        ELSE groundloop_m5_matching_logical_scalar('str',artifact_value.direct_refute_observation_id) END,
      groundloop_m5_matching_logical_scalar('str',artifact_value.certificate_version),
      groundloop_m5_matching_logical_scalar('str',artifact_value.certificate_digest::text)];
    RETURN groundloop_m5_matching_logical_dataclass(
      'ClaimCertificateArtifact',names_value,values_value);
  END IF;
  RAISE EXCEPTION 'unsupported certificate journal kind %',kind_value;
END;
$$;

CREATE FUNCTION groundloop_m5_matching_working_point_preimage(
  relation_value text,row_value jsonb
) RETURNS bytea LANGUAGE plpgsql IMMUTABLE STRICT PARALLEL SAFE AS $$
DECLARE fields_value text[];
DECLARE array_value jsonb;
DECLARE child_value jsonb;
BEGIN
  fields_value:=CASE relation_value
    WHEN 'groundloop_m5_matching_observation_working' THEN ARRAY[
      'sequence','int','9','enum','working','int',row_value->>'epoch_id',
      'text',row_value->>'observation_id','text',row_value->>'requirement_version_id',
      'text',row_value->>'group_version_id','int',row_value->>'requirement_ordinal',
      'sha256',row_value->>'text_hash','bool',CASE WHEN (row_value->>'present')::boolean THEN '1' ELSE '0' END,
      'int',row_value->>'updated_revision']
    WHEN 'groundloop_m5_matching_edge_working' THEN ARRAY[
      'sequence','int','8','enum','working','int',row_value->>'epoch_id',
      'text',row_value->>'requirement_version_id','sha256',row_value->>'text_hash',
      'text',row_value->>'group_version_id','int',row_value->>'requirement_ordinal',
      'int',row_value->>'refcount','int',row_value->>'updated_revision']
    WHEN 'groundloop_m5_matching_hash_mask_working' THEN ARRAY[
      'sequence','int','6','enum','working','int',row_value->>'epoch_id',
      'text',row_value->>'group_version_id','sha256',row_value->>'text_hash',
      'int',row_value->>'mask','int',row_value->>'updated_revision']
    WHEN 'groundloop_m5_matching_hall_working' THEN ARRAY[
      'sequence','int','12','enum','working','int',row_value->>'epoch_id',
      'text',row_value->>'group_version_id','bool',CASE WHEN (row_value->>'present')::boolean THEN '1' ELSE '0' END]
    ELSE NULL END;
  IF fields_value IS NULL THEN RAISE EXCEPTION 'unsupported physical journal row'; END IF;
  IF relation_value='groundloop_m5_matching_hall_working' THEN
    IF (row_value->>'present')::boolean THEN
      fields_value:=fields_value||ARRAY['int',row_value->>'requirement_count'];
      FOREACH array_value IN ARRAY ARRAY[row_value->'mask_histogram',row_value->'neighbor_counts',row_value->'deficiencies'] LOOP
        fields_value:=fields_value||ARRAY['sequence','int',jsonb_array_length(array_value)::text];
        FOR child_value IN SELECT entry.value FROM jsonb_array_elements(array_value) entry(value)
        LOOP fields_value:=fields_value||ARRAY['int',child_value::text]; END LOOP;
      END LOOP;
      fields_value:=fields_value||ARRAY['int',row_value->>'maximum_deficiency',
        'int',row_value->>'matching_size','int',row_value->>'distinct_hash_count'];
    ELSE
      fields_value:=fields_value||ARRAY['null','null','null','null','null','null','null'];
    END IF;
    fields_value:=fields_value||ARRAY['int',row_value->>'updated_revision'];
  END IF;
  RETURN groundloop_m5_matching_encode_fields(fields_value);
END;
$$;

CREATE FUNCTION groundloop_m5_matching_current_point_preimage(
  working_relation_value text,row_value jsonb
) RETURNS bytea LANGUAGE plpgsql IMMUTABLE STRICT PARALLEL SAFE AS $$
DECLARE fields_value text[];
DECLARE array_value jsonb;
DECLARE child_value jsonb;
BEGIN
  fields_value:=CASE working_relation_value
    WHEN 'groundloop_m5_matching_observation_working' THEN ARRAY[
      'sequence','int','8','enum','current','text',row_value->>'observation_id',
      'text',row_value->>'requirement_version_id','text',row_value->>'group_version_id',
      'int',row_value->>'requirement_ordinal','sha256',row_value->>'text_hash',
      'int',row_value->>'installed_epoch_id','int',row_value->>'installed_revision']
    WHEN 'groundloop_m5_matching_edge_working' THEN ARRAY[
      'sequence','int','8','enum','current','text',row_value->>'requirement_version_id',
      'sha256',row_value->>'text_hash','text',row_value->>'group_version_id',
      'int',row_value->>'requirement_ordinal','int',row_value->>'refcount',
      'int',row_value->>'installed_epoch_id','int',row_value->>'installed_revision']
    WHEN 'groundloop_m5_matching_hash_mask_working' THEN ARRAY[
      'sequence','int','6','enum','current','text',row_value->>'group_version_id',
      'sha256',row_value->>'text_hash','int',row_value->>'mask',
      'int',row_value->>'installed_epoch_id','int',row_value->>'installed_revision']
    WHEN 'groundloop_m5_matching_hall_working' THEN ARRAY[
      'sequence','int','11','enum','current','text',row_value->>'group_version_id',
      'int',row_value->>'requirement_count']
    ELSE NULL END;
  IF fields_value IS NULL THEN RAISE EXCEPTION 'unsupported current physical journal row'; END IF;
  IF working_relation_value='groundloop_m5_matching_hall_working' THEN
    FOREACH array_value IN ARRAY ARRAY[row_value->'mask_histogram',row_value->'neighbor_counts',row_value->'deficiencies'] LOOP
      fields_value:=fields_value||ARRAY['sequence','int',jsonb_array_length(array_value)::text];
      FOR child_value IN SELECT entry.value FROM jsonb_array_elements(array_value) entry(value)
      LOOP fields_value:=fields_value||ARRAY['int',child_value::text]; END LOOP;
    END LOOP;
    fields_value:=fields_value||ARRAY['int',row_value->>'maximum_deficiency',
      'int',row_value->>'matching_size','int',row_value->>'distinct_hash_count',
      'int',row_value->>'installed_epoch_id','int',row_value->>'installed_revision'];
  END IF;
  RETURN groundloop_m5_matching_encode_fields(fields_value);
END;
$$;

CREATE FUNCTION groundloop_m5_matching_working_row_matches_point(
    relation_value text, row_value jsonb, decoded_change jsonb,
    expected_epoch bigint, expected_revision bigint,
    point_name text DEFAULT 'after'
) RETURNS boolean LANGUAGE plpgsql IMMUTABLE PARALLEL SAFE AS $$
DECLARE point_value jsonb:=decoded_change->point_name;
DECLARE fields_value jsonb:=point_value->'node'->'children';
BEGIN
  IF row_value IS NULL OR point_value='null'::jsonb
     OR point_value->>'layer'<>'working'
     OR (row_value->>'epoch_id')::bigint<>expected_epoch
     OR (row_value->>'updated_revision')::bigint<>expected_revision
  THEN RETURN false; END IF;
  RETURN CASE relation_value
    WHEN 'groundloop_m5_matching_observation_working' THEN
      row_value->>'observation_id'=decoded_change->>'outer_one'
      AND row_value->>'requirement_version_id'=fields_value->3->>'value'
      AND row_value->>'group_version_id'=fields_value->4->>'value'
      AND (row_value->>'requirement_ordinal')::integer=(fields_value->5->>'value')::integer
      AND row_value->>'text_hash'=fields_value->6->>'value'
      AND (row_value->>'present')::boolean=(fields_value->7->>'value')::boolean
    WHEN 'groundloop_m5_matching_edge_working' THEN
      row_value->>'requirement_version_id'=decoded_change->>'outer_one'
      AND row_value->>'text_hash'=decoded_change->>'outer_two'
      AND row_value->>'group_version_id'=fields_value->4->>'value'
      AND (row_value->>'requirement_ordinal')::integer=(fields_value->5->>'value')::integer
      AND (row_value->>'refcount')::bigint=(fields_value->6->>'value')::bigint
    WHEN 'groundloop_m5_matching_hash_mask_working' THEN
      row_value->>'group_version_id'=decoded_change->>'outer_one'
      AND row_value->>'text_hash'=decoded_change->>'outer_two'
      AND (row_value->>'mask')::integer=(fields_value->4->>'value')::integer
    WHEN 'groundloop_m5_matching_hall_working' THEN
      row_value->>'group_version_id'=decoded_change->>'outer_one'
      AND (row_value->>'present')::boolean=(fields_value->3->>'value')::boolean
      AND CASE WHEN (fields_value->3->>'value')::boolean THEN
        (row_value->>'requirement_count')::integer=(fields_value->4->>'value')::integer
        AND row_value->'mask_histogram'=to_jsonb(groundloop_m5_matching_json_int_array(fields_value->5))
        AND row_value->'neighbor_counts'=to_jsonb(groundloop_m5_matching_json_int_array(fields_value->6))
        AND row_value->'deficiencies'=to_jsonb(groundloop_m5_matching_json_int_array(fields_value->7))
        AND (row_value->>'maximum_deficiency')::integer=(fields_value->8->>'value')::integer
        AND (row_value->>'matching_size')::integer=(fields_value->9->>'value')::integer
        AND (row_value->>'distinct_hash_count')::bigint=(fields_value->10->>'value')::bigint
      ELSE row_value->'requirement_count'='null'::jsonb
        AND row_value->'mask_histogram'='null'::jsonb
        AND row_value->'neighbor_counts'='null'::jsonb
        AND row_value->'deficiencies'='null'::jsonb
        AND row_value->'maximum_deficiency'='null'::jsonb
        AND row_value->'matching_size'='null'::jsonb
        AND row_value->'distinct_hash_count'='null'::jsonb END
    ELSE false END;
END;
$$;

CREATE FUNCTION groundloop_m5_matching_current_row_matches_point(
    relation_value text, row_value jsonb, decoded_change jsonb
) RETURNS boolean LANGUAGE plpgsql IMMUTABLE PARALLEL SAFE AS $$
DECLARE point_value jsonb:=decoded_change->'before';
DECLARE fields_value jsonb:=point_value->'node'->'children';
BEGIN
  IF row_value IS NULL OR point_value='null'::jsonb
     OR point_value->>'layer'<>'current' THEN RETURN false; END IF;
  RETURN CASE relation_value
    WHEN 'groundloop_m5_matching_observation_working' THEN
      row_value->>'observation_id'=decoded_change->>'outer_one'
      AND row_value->>'requirement_version_id'=fields_value->2->>'value'
      AND row_value->>'group_version_id'=fields_value->3->>'value'
      AND (row_value->>'requirement_ordinal')::integer=(fields_value->4->>'value')::integer
      AND row_value->>'text_hash'=fields_value->5->>'value'
      AND (row_value->>'installed_epoch_id')::bigint=(fields_value->6->>'value')::bigint
      AND (row_value->>'installed_revision')::bigint=(fields_value->7->>'value')::bigint
    WHEN 'groundloop_m5_matching_edge_working' THEN
      row_value->>'requirement_version_id'=decoded_change->>'outer_one'
      AND row_value->>'text_hash'=decoded_change->>'outer_two'
      AND row_value->>'group_version_id'=fields_value->3->>'value'
      AND (row_value->>'requirement_ordinal')::integer=(fields_value->4->>'value')::integer
      AND (row_value->>'refcount')::bigint=(fields_value->5->>'value')::bigint
      AND (row_value->>'installed_epoch_id')::bigint=(fields_value->6->>'value')::bigint
      AND (row_value->>'installed_revision')::bigint=(fields_value->7->>'value')::bigint
    WHEN 'groundloop_m5_matching_hash_mask_working' THEN
      row_value->>'group_version_id'=decoded_change->>'outer_one'
      AND row_value->>'text_hash'=decoded_change->>'outer_two'
      AND (row_value->>'mask')::integer=(fields_value->3->>'value')::integer
      AND (row_value->>'installed_epoch_id')::bigint=(fields_value->4->>'value')::bigint
      AND (row_value->>'installed_revision')::bigint=(fields_value->5->>'value')::bigint
    WHEN 'groundloop_m5_matching_hall_working' THEN
      row_value->>'group_version_id'=decoded_change->>'outer_one'
      AND (row_value->>'requirement_count')::integer=(fields_value->2->>'value')::integer
      AND row_value->'mask_histogram'=to_jsonb(groundloop_m5_matching_json_int_array(fields_value->3))
      AND row_value->'neighbor_counts'=to_jsonb(groundloop_m5_matching_json_int_array(fields_value->4))
      AND row_value->'deficiencies'=to_jsonb(groundloop_m5_matching_json_int_array(fields_value->5))
      AND (row_value->>'maximum_deficiency')::integer=(fields_value->6->>'value')::integer
      AND (row_value->>'matching_size')::integer=(fields_value->7->>'value')::integer
      AND (row_value->>'distinct_hash_count')::bigint=(fields_value->8->>'value')::bigint
      AND (row_value->>'installed_epoch_id')::bigint=(fields_value->9->>'value')::bigint
      AND (row_value->>'installed_revision')::bigint=(fields_value->10->>'value')::bigint
    ELSE false END;
END;
$$;

CREATE FUNCTION groundloop_m5_matching_logical_row_matches(
    kind_value text, row_value jsonb, output_value jsonb,
    expected_epoch bigint, expected_revision bigint,
    expected_policy text
) RETURNS boolean LANGUAGE plpgsql IMMUTABLE PARALLEL SAFE AS $$
DECLARE after_value jsonb:=output_value->'after';
DECLARE fields_value jsonb:=after_value->'fields';
DECLARE old_score jsonb;
BEGIN
  IF row_value IS NULL OR after_value->>'tag'<>'dataclass' THEN RETURN false; END IF;
  RETURN CASE kind_value
    WHEN 'requirement_state' THEN
      (row_value->>'epoch_id')::bigint=expected_epoch
      AND row_value->>'requirement_version_id'=output_value->>'object_id'
      AND row_value->'witness_hashes'=to_jsonb(groundloop_m5_matching_json_text_array(fields_value->1->'value'))
      AND row_value->'supporting_observation_ids'=to_jsonb(groundloop_m5_matching_json_text_array(fields_value->2->'value'))
      AND (row_value->>'witness_count')::integer=(fields_value->3->'value'->>'value')::integer
      AND (row_value->>'satisfied')::boolean=(fields_value->4->'value'->>'value')::boolean
      AND row_value->>'decision_policy_version'=expected_policy
      AND (row_value->>'updated_revision')::bigint=expected_revision
    WHEN 'group_state' THEN
      (row_value->>'epoch_id')::bigint=expected_epoch
      AND row_value->>'group_version_id'=output_value->>'object_id'
      AND (row_value->>'requirement_count')::integer=(fields_value->1->'value'->>'value')::integer
      AND (row_value->>'satisfied_count')::integer=(fields_value->2->'value'->>'value')::integer
      AND (row_value->>'matching_size')::integer=(fields_value->3->'value'->>'value')::integer
      AND (row_value->>'complete')::boolean=(fields_value->4->'value'->>'value')::boolean
      AND row_value->>'decision_policy_version'=expected_policy
      AND (row_value->>'updated_revision')::bigint=expected_revision
    WHEN 'claim_state' THEN
      (row_value->>'epoch_id')::bigint=expected_epoch
      AND row_value->>'claim_id'=output_value->>'object_id'
      AND (row_value->>'support_count')::integer=(fields_value->1->'value'->>'value')::integer
      AND (row_value->>'refute_count')::integer=(fields_value->2->'value'->>'value')::integer
      AND CASE WHEN fields_value->3->'value'->>'tag'='none'
        THEN row_value->'best_support_score'='null'::jsonb
        ELSE encode(float8send((row_value->>'best_support_score')::double precision),'hex')=
             fields_value->3->'value'->>'value' END
      AND CASE WHEN fields_value->4->'value'->>'tag'='none'
        THEN row_value->'best_refute_score'='null'::jsonb
        ELSE encode(float8send((row_value->>'best_refute_score')::double precision),'hex')=
             fields_value->4->'value'->>'value' END
      AND row_value->'supporting_observation_ids'=to_jsonb(groundloop_m5_matching_json_text_array(fields_value->5->'value'))
      AND row_value->'refuting_observation_ids'=to_jsonb(groundloop_m5_matching_json_text_array(fields_value->6->'value'))
      AND (row_value->>'complete_group_count')::integer=(fields_value->7->'value'->>'value')::integer
      AND row_value->'complete_group_ids'=to_jsonb(groundloop_m5_matching_json_text_array(fields_value->8->'value'))
      AND row_value->>'status'=fields_value->9->'value'->>'value'
      AND row_value->>'decision_policy_version'=expected_policy
      AND (row_value->>'updated_revision')::bigint=expected_revision
    WHEN 'answer_state' THEN
      (row_value->>'epoch_id')::bigint=expected_epoch
      AND row_value->>'answer_version_id'=output_value->>'object_id'
      AND (row_value->>'required_claim_count')::integer=(fields_value->1->'value'->>'value')::integer
      AND (row_value->>'supported_count')::integer=(fields_value->2->'value'->>'value')::integer
      AND (row_value->>'unsupported_count')::integer=(fields_value->3->'value'->>'value')::integer
      AND (row_value->>'refuted_count')::integer=(fields_value->4->'value'->>'value')::integer
      AND (row_value->>'conflicted_count')::integer=(fields_value->5->'value'->>'value')::integer
      AND row_value->>'status'=fields_value->6->'value'->>'value'
      AND (row_value->>'updated_revision')::bigint=expected_revision
    WHEN 'group_binding' THEN
      (row_value->>'epoch_id')::bigint=expected_epoch
      AND row_value->>'group_version_id'=output_value->>'object_id'
      AND (row_value->>'valid_from_revision')::bigint=(fields_value->2->'value'->>'value')::bigint
      AND CASE WHEN fields_value->3->'value'->>'tag'='none'
        THEN row_value->'valid_to_revision'='null'::jsonb
        ELSE (row_value->>'valid_to_revision')::bigint=(fields_value->3->'value'->>'value')::bigint END
      AND row_value->>'certificate_digest'=fields_value->4->'value'->>'value'
    WHEN 'claim_binding' THEN
      (row_value->>'epoch_id')::bigint=expected_epoch
      AND row_value->>'claim_id'=output_value->>'object_id'
      AND (row_value->>'valid_from_revision')::bigint=(fields_value->2->'value'->>'value')::bigint
      AND CASE WHEN fields_value->3->'value'->>'tag'='none'
        THEN row_value->'valid_to_revision'='null'::jsonb
        ELSE (row_value->>'valid_to_revision')::bigint=(fields_value->3->'value'->>'value')::bigint END
      AND row_value->>'certificate_digest'=fields_value->4->'value'->>'value'
    ELSE false END;
END;
$$;

CREATE FUNCTION groundloop_m5_matching_begin_transition_context(
    selected_epoch_id bigint, expected_runtime_revision bigint,
    resulting_revision bigint, selected_source_kind text, selected_source_id text
) RETURNS void LANGUAGE plpgsql SECURITY DEFINER SET search_path FROM CURRENT AS $$
BEGIN
  IF to_regclass('pg_temp.groundloop_m5_matching_transition_context') IS NOT NULL
     OR to_regclass('pg_temp.groundloop_m5_matching_change_journal') IS NOT NULL
     OR to_regclass('pg_temp.groundloop_m5_matching_expected_changes') IS NOT NULL
     OR to_regclass('pg_temp.groundloop_m5_matching_promotion_context') IS NOT NULL
     OR to_regclass('pg_temp.groundloop_m5_matching_promotion_journal') IS NOT NULL
     OR to_regclass('pg_temp.groundloop_m5_matching_promotion_expected') IS NOT NULL
     OR nullif(current_setting('groundloop.m5_matching_mode',true),'') IS NOT NULL
     OR nullif(current_setting('groundloop.m5_matching_context_oid',true),'') IS NOT NULL
     OR nullif(current_setting('groundloop.m5_matching_journal_oid',true),'') IS NOT NULL
     OR nullif(current_setting('groundloop.m5_matching_expected_oid',true),'') IS NOT NULL
  THEN RAISE EXCEPTION 'persisted matching transition context already exists'; END IF;
  EXECUTE 'CREATE TEMP TABLE pg_temp.groundloop_m5_matching_transition_context (
    backend_pid integer NOT NULL, transaction_id bigint NOT NULL,
    session_role text NOT NULL, epoch_id bigint NOT NULL,
    expected_revision bigint NOT NULL, resulting_revision bigint NOT NULL,
    source_kind text NOT NULL, source_id text NOT NULL,
    anchor_count integer NOT NULL DEFAULT 0,
    runtime_first_old jsonb, runtime_final_new jsonb,
    runtime_saw_insert boolean NOT NULL DEFAULT false,
    runtime_saw_update boolean NOT NULL DEFAULT false,
    validation_started boolean NOT NULL DEFAULT false,
    validation_done boolean NOT NULL DEFAULT false,
    PRIMARY KEY (backend_pid,transaction_id,session_role)
  ) ON COMMIT DROP';
  EXECUTE 'CREATE TEMP TABLE pg_temp.groundloop_m5_matching_change_journal (
    relation_name text NOT NULL, key_preimage bytea NOT NULL,
    first_old jsonb, final_new jsonb, first_operation text NOT NULL,
    last_operation text NOT NULL, mutation_count integer NOT NULL,
    saw_insert boolean NOT NULL, saw_update boolean NOT NULL,
    saw_delete boolean NOT NULL,
    PRIMARY KEY(relation_name,key_preimage)
  ) ON COMMIT DROP';
  EXECUTE 'CREATE TEMP TABLE pg_temp.groundloop_m5_matching_expected_changes (
    relation_name text NOT NULL, key_preimage bytea NOT NULL,
    PRIMARY KEY(relation_name,key_preimage)
  ) ON COMMIT DROP';
  EXECUTE 'INSERT INTO pg_temp.groundloop_m5_matching_transition_context
    (backend_pid,transaction_id,session_role,epoch_id,expected_revision,
     resulting_revision,source_kind,source_id)
    VALUES ($1,pg_current_xact_id()::text::bigint,session_user,$2,$3,$4,$5,$6)'
    USING pg_backend_pid(),selected_epoch_id,expected_runtime_revision,
          resulting_revision,selected_source_kind,selected_source_id;
  PERFORM set_config('groundloop.m5_matching_context_oid',
    to_regclass('pg_temp.groundloop_m5_matching_transition_context')::oid::text,true);
  PERFORM set_config('groundloop.m5_matching_journal_oid',
    to_regclass('pg_temp.groundloop_m5_matching_change_journal')::oid::text,true);
  PERFORM set_config('groundloop.m5_matching_expected_oid',
    to_regclass('pg_temp.groundloop_m5_matching_expected_changes')::oid::text,true);
END;
$$;

CREATE FUNCTION groundloop_m5_matching_journal_change(
    relation_value text, operation_value text, old_value jsonb, new_value jsonb
) RETURNS void LANGUAGE plpgsql SECURITY DEFINER SET search_path FROM CURRENT AS $$
DECLARE row_value jsonb:=coalesce(new_value,old_value);
DECLARE key_value bytea;
DECLARE context_epoch bigint;
DECLARE context_result bigint;
DECLARE context_started boolean;
BEGIN
  IF coalesce(current_setting('groundloop.m5_matching_mode',true),'')<>'transition' THEN
    IF to_regclass('pg_temp.groundloop_m5_matching_transition_context') IS NOT NULL
       OR to_regclass('pg_temp.groundloop_m5_matching_promotion_context') IS NOT NULL
       OR to_regclass('pg_temp.groundloop_m5_matching_promotion_journal') IS NOT NULL
       OR to_regclass('pg_temp.groundloop_m5_matching_promotion_expected') IS NOT NULL
       OR nullif(current_setting('groundloop.m5_matching_context_oid',true),'') IS NOT NULL
       OR nullif(current_setting('groundloop.m5_matching_journal_oid',true),'') IS NOT NULL
       OR nullif(current_setting('groundloop.m5_matching_expected_oid',true),'') IS NOT NULL
    THEN RAISE EXCEPTION 'persisted matching journal mode/context escape'; END IF;
    RETURN;
  END IF;
  IF to_regclass('pg_temp.groundloop_m5_matching_transition_context') IS NULL
     OR to_regclass('pg_temp.groundloop_m5_matching_change_journal') IS NULL
     OR to_regclass('pg_temp.groundloop_m5_matching_expected_changes') IS NULL
  THEN RAISE EXCEPTION 'persisted matching transition lacks private context'; END IF;
  EXECUTE 'SELECT epoch_id,resulting_revision,validation_started
             FROM pg_temp.groundloop_m5_matching_transition_context
            WHERE backend_pid=$1 AND transaction_id=pg_current_xact_id()::text::bigint
              AND session_role=session_user'
    INTO STRICT context_epoch,context_result,context_started USING pg_backend_pid();
  IF context_started THEN RAISE EXCEPTION 'persisted matching DML after validation'; END IF;
  IF coalesce((row_value->>'epoch_id')::bigint,context_epoch)<>context_epoch THEN
    RAISE EXCEPTION 'persisted matching journal cross-epoch row'; END IF;
  key_value:=groundloop_m5_matching_row_key(relation_value,row_value);
  IF operation_value='UPDATE' AND
     groundloop_m5_matching_row_key(relation_value,old_value)<>key_value THEN
    RAISE EXCEPTION 'persisted matching journal key cannot mutate'; END IF;
  EXECUTE 'INSERT INTO pg_temp.groundloop_m5_matching_change_journal AS journal
      (relation_name,key_preimage,first_old,final_new,first_operation,last_operation,
       mutation_count,saw_insert,saw_update,saw_delete)
    VALUES ($1,$2,$3,$4,$5,$5,1,$5=''INSERT'',$5=''UPDATE'',$5=''DELETE'')
    ON CONFLICT (relation_name,key_preimage) DO UPDATE SET
      final_new=EXCLUDED.final_new,last_operation=EXCLUDED.last_operation,
      mutation_count=journal.mutation_count+1,
      saw_insert=journal.saw_insert OR EXCLUDED.saw_insert,
      saw_update=journal.saw_update OR EXCLUDED.saw_update,
      saw_delete=journal.saw_delete OR EXCLUDED.saw_delete'
    USING relation_value,key_value,old_value,new_value,operation_value;
END;
$$;

-- Preserve every migration-014 mutation check while extending its existing
-- trigger surface with transaction-local D25 journaling. Currency retains the
-- exact migration-014 behavior and is deliberately outside the D25 journal;
-- no new trigger lock is introduced for working state or bindings.
CREATE OR REPLACE FUNCTION groundloop_m5_validate_working_state_mutation()
RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path FROM CURRENT AS $$
DECLARE current_revision bigint;
BEGIN
  IF TG_OP='DELETE' THEN RAISE EXCEPTION 'M5 working state cannot be deleted'; END IF;
  PERFORM groundloop_m5_assert_writable_epoch(NEW.epoch_id);
  SELECT revision INTO STRICT current_revision FROM groundloop_epoch
    WHERE epoch_id=NEW.epoch_id;
  IF NEW.updated_revision<>current_revision THEN
    RAISE EXCEPTION 'M5 working state must name the current epoch revision'; END IF;
  IF TG_OP='UPDATE' THEN
    IF NEW.epoch_id<>OLD.epoch_id THEN
      RAISE EXCEPTION 'M5 working-state epoch identity cannot change'; END IF;
    IF NEW.updated_revision<=OLD.updated_revision THEN
      RAISE EXCEPTION 'M5 working-state revision must increase'; END IF;
  END IF;
  PERFORM groundloop_m5_matching_journal_change(
    TG_TABLE_NAME,TG_OP,CASE WHEN TG_OP='UPDATE' THEN to_jsonb(OLD) END,to_jsonb(NEW));
  RETURN NEW;
END;
$$;

CREATE OR REPLACE FUNCTION groundloop_m5_validate_revision_interval_mutation()
RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path FROM CURRENT AS $$
DECLARE current_revision bigint;
BEGIN
  IF TG_OP='DELETE' THEN RAISE EXCEPTION 'M5 revision history cannot be deleted'; END IF;
  PERFORM groundloop_m5_assert_writable_epoch(NEW.epoch_id);
  SELECT revision INTO STRICT current_revision FROM groundloop_epoch
    WHERE epoch_id=NEW.epoch_id;
  IF TG_OP='INSERT' AND NEW.valid_from_revision<>current_revision THEN
    RAISE EXCEPTION 'M5 revision history must open at the current revision'; END IF;
  IF TG_OP='UPDATE' THEN
    IF (to_jsonb(NEW)-'valid_to_revision') IS DISTINCT FROM
       (to_jsonb(OLD)-'valid_to_revision')
       OR OLD.valid_to_revision IS NOT NULL OR NEW.valid_to_revision IS NULL
       OR NEW.valid_to_revision<=OLD.valid_from_revision
       OR NEW.valid_to_revision<>current_revision
    THEN RAISE EXCEPTION 'M5 revision history may close exactly once'; END IF;
  END IF;
  IF TG_TABLE_NAME IN (
    'groundloop_m5_working_group_certificate_binding',
    'groundloop_m5_working_claim_certificate_binding') THEN
    PERFORM groundloop_m5_matching_journal_change(
      TG_TABLE_NAME,TG_OP,CASE WHEN TG_OP='UPDATE' THEN to_jsonb(OLD) END,to_jsonb(NEW));
  END IF;
  RETURN NEW;
END;
$$;

CREATE FUNCTION groundloop_m5_matching_artifact_journal_guard()
RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path FROM CURRENT AS $$
BEGIN
  PERFORM groundloop_m5_matching_journal_change(TG_TABLE_NAME,TG_OP,NULL,to_jsonb(NEW));
  RETURN NEW;
END;
$$;

CREATE FUNCTION groundloop_m5_matching_begin_promotion_context(
    selected_mode text, selected_epoch_id bigint, expected_revision bigint,
    resulting_revision bigint, selected_policy text
) RETURNS void LANGUAGE plpgsql SECURITY DEFINER SET search_path FROM CURRENT AS $$
BEGIN
  IF selected_mode NOT IN ('seal','activation')
     OR to_regclass('pg_temp.groundloop_m5_matching_promotion_context') IS NOT NULL
     OR to_regclass('pg_temp.groundloop_m5_matching_promotion_journal') IS NOT NULL
     OR to_regclass('pg_temp.groundloop_m5_matching_promotion_expected') IS NOT NULL
     OR to_regclass('pg_temp.groundloop_m5_matching_transition_context') IS NOT NULL
     OR to_regclass('pg_temp.groundloop_m5_matching_change_journal') IS NOT NULL
     OR to_regclass('pg_temp.groundloop_m5_matching_expected_changes') IS NOT NULL
     OR nullif(current_setting('groundloop.m5_matching_mode',true),'') IS NOT NULL
     OR nullif(current_setting('groundloop.m5_matching_context_oid',true),'') IS NOT NULL
     OR nullif(current_setting('groundloop.m5_matching_journal_oid',true),'') IS NOT NULL
     OR nullif(current_setting('groundloop.m5_matching_expected_oid',true),'') IS NOT NULL
  THEN RAISE EXCEPTION 'persisted matching promotion context already exists'; END IF;
  EXECUTE 'CREATE TEMP TABLE pg_temp.groundloop_m5_matching_promotion_context (
    backend_pid integer NOT NULL, transaction_id bigint NOT NULL,
    session_role text NOT NULL, mode text NOT NULL,
    epoch_id bigint NOT NULL, expected_revision bigint NOT NULL,
    resulting_revision bigint NOT NULL, policy_version text NOT NULL,
    validation_started boolean NOT NULL DEFAULT false,
    validation_done boolean NOT NULL DEFAULT false,
    PRIMARY KEY(backend_pid,transaction_id,session_role)
  ) ON COMMIT DROP';
  EXECUTE 'CREATE TEMP TABLE pg_temp.groundloop_m5_matching_promotion_journal (
    relation_name text NOT NULL,key_preimage bytea NOT NULL,
    first_old jsonb,final_new jsonb,first_operation text NOT NULL,
    last_operation text NOT NULL,mutation_count integer NOT NULL,
    saw_insert boolean NOT NULL,saw_update boolean NOT NULL,
    saw_delete boolean NOT NULL,PRIMARY KEY(relation_name,key_preimage)
  ) ON COMMIT DROP';
  EXECUTE 'CREATE TEMP TABLE pg_temp.groundloop_m5_matching_promotion_expected (
    relation_name text NOT NULL,key_preimage bytea NOT NULL,
    PRIMARY KEY(relation_name,key_preimage)
  ) ON COMMIT DROP';
  EXECUTE 'INSERT INTO pg_temp.groundloop_m5_matching_promotion_context
    VALUES ($1,pg_current_xact_id()::text::bigint,session_user,$2,$3,$4,$5,$6,false,false)'
    USING pg_backend_pid(),selected_mode,selected_epoch_id,expected_revision,
          resulting_revision,selected_policy;
  PERFORM set_config('groundloop.m5_matching_context_oid',
    to_regclass('pg_temp.groundloop_m5_matching_promotion_context')::oid::text,true);
  PERFORM set_config('groundloop.m5_matching_journal_oid',
    to_regclass('pg_temp.groundloop_m5_matching_promotion_journal')::oid::text,true);
  PERFORM set_config('groundloop.m5_matching_expected_oid',
    to_regclass('pg_temp.groundloop_m5_matching_promotion_expected')::oid::text,true);
END;
$$;

CREATE FUNCTION groundloop_m5_matching_journal_promotion_change(
    relation_value text, operation_value text, old_value jsonb, new_value jsonb
) RETURNS void LANGUAGE plpgsql SECURITY DEFINER SET search_path FROM CURRENT AS $$
DECLARE row_value jsonb:=coalesce(new_value,old_value);
DECLARE key_value bytea;
DECLARE context_started boolean;
BEGIN
  IF coalesce(current_setting('groundloop.m5_matching_mode',true),'')
       NOT IN ('seal','activation')
     OR to_regclass('pg_temp.groundloop_m5_matching_promotion_context') IS NULL
     OR to_regclass('pg_temp.groundloop_m5_matching_promotion_journal') IS NULL
     OR to_regclass('pg_temp.groundloop_m5_matching_promotion_expected') IS NULL
  THEN RAISE EXCEPTION 'persisted matching promotion lacks private context'; END IF;
  IF to_regclass('pg_temp.groundloop_m5_matching_promotion_context')::oid::text
          IS DISTINCT FROM current_setting('groundloop.m5_matching_context_oid',true)
     OR to_regclass('pg_temp.groundloop_m5_matching_promotion_journal')::oid::text
          IS DISTINCT FROM current_setting('groundloop.m5_matching_journal_oid',true)
     OR to_regclass('pg_temp.groundloop_m5_matching_promotion_expected')::oid::text
          IS DISTINCT FROM current_setting('groundloop.m5_matching_expected_oid',true)
  THEN RAISE EXCEPTION 'persisted matching promotion private context was replaced'; END IF;
  EXECUTE 'SELECT validation_started
    FROM pg_temp.groundloop_m5_matching_promotion_context
    WHERE backend_pid=$1 AND transaction_id=pg_current_xact_id()::text::bigint
      AND session_role=session_user AND mode=$2
      AND epoch_id=current_setting(''groundloop.m5_matching_epoch_id'')::bigint
      AND expected_revision=current_setting(''groundloop.m5_matching_expected_revision'')::bigint
      AND resulting_revision=current_setting(''groundloop.m5_matching_resulting_revision'')::bigint
      AND policy_version=current_setting(''groundloop.m5_matching_policy'')'
    INTO STRICT context_started USING pg_backend_pid(),
      current_setting('groundloop.m5_matching_mode');
  IF context_started THEN RAISE EXCEPTION 'persisted matching DML after promotion validation'; END IF;
  key_value:=groundloop_m5_matching_row_key(relation_value,row_value);
  IF operation_value='UPDATE' AND
     groundloop_m5_matching_row_key(relation_value,old_value)<>key_value THEN
    RAISE EXCEPTION 'persisted matching promotion key cannot mutate'; END IF;
  EXECUTE 'INSERT INTO pg_temp.groundloop_m5_matching_promotion_journal AS journal
    VALUES ($1,$2,$3,$4,$5,$5,1,$5=''INSERT'',$5=''UPDATE'',$5=''DELETE'')
    ON CONFLICT (relation_name,key_preimage) DO UPDATE SET
      final_new=EXCLUDED.final_new,last_operation=EXCLUDED.last_operation,
      mutation_count=journal.mutation_count+1,
      saw_insert=journal.saw_insert OR EXCLUDED.saw_insert,
      saw_update=journal.saw_update OR EXCLUDED.saw_update,
      saw_delete=journal.saw_delete OR EXCLUDED.saw_delete'
    USING relation_value,key_value,old_value,new_value,operation_value;
END;
$$;

CREATE TRIGGER groundloop_m5_group_certificate_artifact_d25_journal
BEFORE INSERT ON groundloop_m5_group_certificate_artifact
FOR EACH ROW EXECUTE FUNCTION groundloop_m5_matching_artifact_journal_guard();
CREATE TRIGGER groundloop_m5_group_certificate_row_d25_journal
BEFORE INSERT ON groundloop_m5_group_certificate_artifact_row
FOR EACH ROW EXECUTE FUNCTION groundloop_m5_matching_artifact_journal_guard();
CREATE TRIGGER groundloop_m5_claim_certificate_artifact_d25_journal
BEFORE INSERT ON groundloop_m5_claim_certificate_artifact
FOR EACH ROW EXECUTE FUNCTION groundloop_m5_matching_artifact_journal_guard();

CREATE FUNCTION groundloop_m5_matching_authorized_guard()
RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path FROM CURRENT AS $$
DECLARE
    mode text := nullif(current_setting('groundloop.m5_matching_mode', true), '');
    row_value jsonb := CASE WHEN TG_OP='DELETE' THEN to_jsonb(OLD) ELSE to_jsonb(NEW) END;
    context_epoch bigint;
    context_result bigint;
BEGIN
    IF mode IN ('seal','activation') AND (
       to_regclass('pg_temp.groundloop_m5_matching_transition_context') IS NOT NULL
       OR to_regclass('pg_temp.groundloop_m5_matching_change_journal') IS NOT NULL
       OR to_regclass('pg_temp.groundloop_m5_matching_expected_changes') IS NOT NULL)
    THEN RAISE EXCEPTION 'persisted matching transition mode/context escape'; END IF;
    IF mode='transition' AND (
       to_regclass('pg_temp.groundloop_m5_matching_promotion_context') IS NOT NULL
       OR to_regclass('pg_temp.groundloop_m5_matching_promotion_journal') IS NOT NULL
       OR to_regclass('pg_temp.groundloop_m5_matching_promotion_expected') IS NOT NULL)
    THEN RAISE EXCEPTION 'persisted matching promotion mode/context escape'; END IF;
    IF coalesce(mode,'') NOT IN ('transition','seal','activation') AND (
       to_regclass('pg_temp.groundloop_m5_matching_transition_context') IS NOT NULL
       OR to_regclass('pg_temp.groundloop_m5_matching_change_journal') IS NOT NULL
       OR to_regclass('pg_temp.groundloop_m5_matching_expected_changes') IS NOT NULL
       OR to_regclass('pg_temp.groundloop_m5_matching_promotion_context') IS NOT NULL
       OR to_regclass('pg_temp.groundloop_m5_matching_promotion_journal') IS NOT NULL
       OR to_regclass('pg_temp.groundloop_m5_matching_promotion_expected') IS NOT NULL
       OR nullif(current_setting('groundloop.m5_matching_context_oid',true),'') IS NOT NULL
       OR nullif(current_setting('groundloop.m5_matching_journal_oid',true),'') IS NOT NULL
       OR nullif(current_setting('groundloop.m5_matching_expected_oid',true),'') IS NOT NULL)
    THEN RAISE EXCEPTION 'persisted matching DML mode/context escape'; END IF;
    IF coalesce(current_setting('groundloop.m5_checked_transition', true), '') <> 'on'
       OR coalesce(mode, '') NOT IN ('transition','seal','activation','migration')
    THEN RAISE EXCEPTION 'unauthorized persisted matching DML on %', TG_TABLE_NAME;
    END IF;
    IF mode='migration' THEN
        IF EXISTS (SELECT 1 FROM groundloop_m5_schema_bundle
          WHERE bundle_id='m5-persisted-matching-schema-bundle-v1') THEN
          RAISE EXCEPTION 'persisted matching migration bypass is closed'; END IF;
        RETURN CASE WHEN TG_OP='DELETE' THEN OLD ELSE NEW END;
    END IF;
    context_epoch := nullif(current_setting('groundloop.m5_matching_epoch_id', true), '')::bigint;
    context_result := nullif(current_setting('groundloop.m5_matching_resulting_revision', true), '')::bigint;
    IF TG_TABLE_NAME IN (
      'groundloop_m5_matching_patch_artifact',
      'groundloop_m5_matching_work_contribution'
    ) AND TG_OP<>'INSERT' THEN
        RAISE EXCEPTION 'immutable persisted matching artifact cannot change';
    END IF;
    IF TG_TABLE_NAME LIKE '%_working' AND TG_OP='DELETE' THEN
        RAISE EXCEPTION 'persisted matching working rows cannot be deleted';
    END IF;
    IF mode='transition' THEN
      IF to_regclass('pg_temp.groundloop_m5_matching_transition_context') IS NULL THEN
        RAISE EXCEPTION 'persisted matching transition lacks private context'; END IF;
      IF TG_TABLE_NAME LIKE '%_current' THEN
        RAISE EXCEPTION 'transition cannot mutate current matching image';
      END IF;
      IF coalesce((row_value->>'epoch_id')::bigint,
                  (row_value->>'resulting_epoch_id')::bigint) IS DISTINCT FROM context_epoch
         OR coalesce((row_value->>'updated_revision')::bigint,
                     (row_value->>'resulting_revision')::bigint) IS DISTINCT FROM context_result
      THEN RAISE EXCEPTION 'persisted matching transition row scope mismatch'; END IF;
      IF TG_TABLE_NAME IN ('groundloop_m5_matching_patch_artifact',
                           'groundloop_m5_matching_work_contribution')
         AND (row_value->>'source_kind' IS DISTINCT FROM current_setting('groundloop.m5_matching_source_kind')
              OR row_value->>'source_id' IS DISTINCT FROM current_setting('groundloop.m5_matching_source_id'))
      THEN RAISE EXCEPTION 'persisted matching transition source mismatch'; END IF;
      PERFORM groundloop_m5_matching_journal_change(
        TG_TABLE_NAME,TG_OP,
        CASE WHEN TG_OP IN ('UPDATE','DELETE') THEN to_jsonb(OLD) END,
        CASE WHEN TG_OP IN ('INSERT','UPDATE') THEN to_jsonb(NEW) END);
    ELSIF mode IN ('seal','activation') THEN
      IF to_regclass('pg_temp.groundloop_m5_matching_promotion_context') IS NULL
         OR to_regclass('pg_temp.groundloop_m5_matching_promotion_journal') IS NULL
         OR to_regclass('pg_temp.groundloop_m5_matching_promotion_expected') IS NULL
      THEN RAISE EXCEPTION 'persisted matching promotion context mismatch'; END IF;
      IF to_regclass('pg_temp.groundloop_m5_matching_promotion_context')::oid::text
              IS DISTINCT FROM current_setting('groundloop.m5_matching_context_oid',true)
         OR to_regclass('pg_temp.groundloop_m5_matching_promotion_journal')::oid::text
              IS DISTINCT FROM current_setting('groundloop.m5_matching_journal_oid',true)
         OR to_regclass('pg_temp.groundloop_m5_matching_promotion_expected')::oid::text
              IS DISTINCT FROM current_setting('groundloop.m5_matching_expected_oid',true)
      THEN RAISE EXCEPTION 'persisted matching promotion context mismatch'; END IF;
      IF NOT EXISTS (
           SELECT 1 FROM pg_temp.groundloop_m5_matching_promotion_context context
            WHERE context.backend_pid=pg_backend_pid()
              AND context.transaction_id=pg_current_xact_id()::text::bigint
              AND context.session_role=session_user
              AND context.mode=current_setting('groundloop.m5_matching_mode')
              AND context.epoch_id=context_epoch
              AND context.expected_revision=
                    current_setting('groundloop.m5_matching_expected_revision')::bigint
              AND context.resulting_revision=context_result
              AND context.policy_version=current_setting('groundloop.m5_matching_policy'))
      THEN RAISE EXCEPTION 'persisted matching promotion context mismatch'; END IF;
      IF TG_TABLE_NAME NOT LIKE '%_current' THEN
        RAISE EXCEPTION '% cannot mutate non-current matching relation', mode;
      END IF;
      IF mode='activation' AND TG_OP<>'INSERT' THEN
        RAISE EXCEPTION 'activation may only insert current matching rows';
      END IF;
      IF mode='seal'
         AND TG_TABLE_NAME='groundloop_m5_matching_image_current'
         AND TG_OP<>'UPDATE' THEN
        RAISE EXCEPTION 'seal must update the current matching image header';
      END IF;
      IF TG_TABLE_NAME='groundloop_m5_matching_image_current'
         AND row_value->>'decision_policy_version'
               IS DISTINCT FROM current_setting('groundloop.m5_matching_policy')
      THEN RAISE EXCEPTION 'persisted matching current image policy mismatch'; END IF;
      IF TG_OP<>'DELETE'
         AND ((row_value->>'installed_epoch_id')::bigint IS DISTINCT FROM context_epoch
              OR (row_value->>'installed_revision')::bigint IS DISTINCT FROM context_result)
      THEN RAISE EXCEPTION 'persisted matching current row scope mismatch'; END IF;
      IF mode='seal' AND TG_OP='DELETE' AND NOT (
        (TG_TABLE_NAME='groundloop_m5_matching_observation_current' AND EXISTS (
          SELECT 1 FROM groundloop_m5_matching_observation_working w
          WHERE w.epoch_id=context_epoch
            AND w.observation_id=row_value->>'observation_id'
            AND NOT w.present))
        OR (TG_TABLE_NAME='groundloop_m5_matching_edge_current' AND EXISTS (
          SELECT 1 FROM groundloop_m5_matching_edge_working w
          WHERE w.epoch_id=context_epoch
            AND w.requirement_version_id=row_value->>'requirement_version_id'
            AND w.text_hash=row_value->>'text_hash' AND w.refcount=0))
        OR (TG_TABLE_NAME='groundloop_m5_matching_hash_mask_current' AND EXISTS (
          SELECT 1 FROM groundloop_m5_matching_hash_mask_working w
          WHERE w.epoch_id=context_epoch
            AND w.group_version_id=row_value->>'group_version_id'
            AND w.text_hash=row_value->>'text_hash' AND w.mask=0))
        OR (TG_TABLE_NAME='groundloop_m5_matching_hall_current' AND EXISTS (
          SELECT 1 FROM groundloop_m5_matching_hall_working w
          WHERE w.epoch_id=context_epoch
            AND w.group_version_id=row_value->>'group_version_id'
            AND NOT w.present))
      ) THEN RAISE EXCEPTION 'seal delete lacks its exact working tombstone'; END IF;
      PERFORM groundloop_m5_matching_journal_promotion_change(
        TG_TABLE_NAME,TG_OP,
        CASE WHEN TG_OP IN ('UPDATE','DELETE') THEN to_jsonb(OLD) END,
        CASE WHEN TG_OP IN ('INSERT','UPDATE') THEN to_jsonb(NEW) END);
    END IF;
    RETURN CASE WHEN TG_OP='DELETE' THEN OLD ELSE NEW END;
END;
$$;

CREATE FUNCTION groundloop_m5_authorize_persisted_matching_transition(
    selected_epoch_id bigint, expected_runtime_revision bigint,
    resulting_revision bigint, selected_source_kind text, selected_source_id text
) RETURNS void LANGUAGE plpgsql SECURITY DEFINER SET search_path FROM CURRENT AS $$
DECLARE locked_epoch_revision bigint;
DECLARE locked_runtime_revision bigint;
BEGIN
    IF coalesce(current_setting('groundloop.m5_checked_transition', true), '') <> 'on'
       OR selected_source_kind NOT IN ('structural_open','requirement_completion','direct_transition')
       OR btrim(selected_source_id) = '' OR expected_runtime_revision < 1
       OR NOT ((selected_source_kind='structural_open' AND expected_runtime_revision=1 AND resulting_revision=1)
          OR (selected_source_kind<>'structural_open' AND resulting_revision=expected_runtime_revision+1))
    THEN RAISE EXCEPTION 'invalid persisted matching transition authorization'; END IF;
    SELECT epoch.revision,runtime.revision
      INTO locked_epoch_revision,locked_runtime_revision
      FROM groundloop_epoch epoch
      JOIN groundloop_m5_runtime_epoch runtime USING(epoch_id)
     WHERE epoch.epoch_id=selected_epoch_id
     FOR UPDATE OF epoch,runtime;
    IF NOT FOUND OR locked_epoch_revision<>expected_runtime_revision
       OR locked_runtime_revision<>expected_runtime_revision
    THEN RAISE EXCEPTION 'persisted matching transition CAS/revision mismatch'; END IF;
    PERFORM groundloop_m5_matching_begin_transition_context(
      selected_epoch_id,expected_runtime_revision,resulting_revision,
      selected_source_kind,selected_source_id);
    PERFORM set_config('groundloop.m5_matching_mode','transition',true);
    PERFORM set_config('groundloop.m5_matching_epoch_id',selected_epoch_id::text,true);
    PERFORM set_config('groundloop.m5_matching_expected_revision',expected_runtime_revision::text,true);
    PERFORM set_config('groundloop.m5_matching_resulting_revision',resulting_revision::text,true);
    PERFORM set_config('groundloop.m5_matching_source_kind',selected_source_kind,true);
    PERFORM set_config('groundloop.m5_matching_source_id',selected_source_id,true);
END;
$$;

CREATE FUNCTION groundloop_m5_authorize_persisted_matching_seal(
    selected_epoch_id bigint, expected_revision bigint, sealed_revision bigint
) RETURNS void LANGUAGE plpgsql SECURITY DEFINER SET search_path FROM CURRENT AS $$
DECLARE selected_policy text;
DECLARE locked_epoch_revision bigint;
DECLARE locked_runtime_revision bigint;
BEGIN
    IF to_regclass('pg_temp.groundloop_m5_matching_transition_context') IS NOT NULL
       OR to_regclass('pg_temp.groundloop_m5_matching_promotion_context') IS NOT NULL
       OR to_regclass('pg_temp.groundloop_m5_matching_promotion_journal') IS NOT NULL
       OR to_regclass('pg_temp.groundloop_m5_matching_promotion_expected') IS NOT NULL
       OR nullif(current_setting('groundloop.m5_matching_mode',true),'') IS NOT NULL
       OR nullif(current_setting('groundloop.m5_matching_context_oid',true),'') IS NOT NULL
       OR nullif(current_setting('groundloop.m5_matching_journal_oid',true),'') IS NOT NULL
       OR nullif(current_setting('groundloop.m5_matching_expected_oid',true),'') IS NOT NULL
       OR coalesce(current_setting('groundloop.m5_checked_transition', true),'') <> 'on'
       OR expected_revision < 1 OR sealed_revision < 1
    THEN RAISE EXCEPTION 'invalid persisted matching seal authorization'; END IF;
    SELECT revision INTO locked_epoch_revision FROM groundloop_epoch
     WHERE epoch_id=selected_epoch_id FOR UPDATE;
    SELECT revision INTO locked_runtime_revision FROM groundloop_m5_runtime_epoch
     WHERE epoch_id=selected_epoch_id FOR UPDATE;
    SELECT image.decision_policy_version INTO selected_policy
      FROM groundloop_m5_matching_image_working image
      JOIN groundloop_m5_update update_row USING(epoch_id)
      JOIN groundloop_m5_matching_work_accumulator accumulator USING(epoch_id)
      JOIN groundloop_m5_matching_image_current current_image ON current_image.singleton
      JOIN groundloop_m4_publication_head m4_head
        ON m4_head.singleton AND m4_head.epoch_id=update_row.previous_published_epoch_id
      JOIN groundloop_m5_publication_head m5_head
        ON m5_head.singleton AND m5_head.epoch_id=m4_head.epoch_id
      JOIN groundloop_epoch predecessor ON predecessor.epoch_id=m5_head.epoch_id
      JOIN groundloop_decision_policy policy
        ON policy.policy_version=image.decision_policy_version
       AND policy.valid_from_epoch<=selected_epoch_id
       AND (policy.valid_to_epoch IS NULL OR selected_epoch_id<policy.valid_to_epoch)
      JOIN groundloop_m5_schema_bundle ledger
        ON ledger.bundle_id='m5-persisted-matching-schema-bundle-v1'
     WHERE image.epoch_id=selected_epoch_id
       AND current_image.installed_epoch_id=m5_head.epoch_id
       AND current_image.installed_revision=m5_head.sealed_revision
       AND image.base_epoch_id=current_image.installed_epoch_id
       AND image.base_revision=current_image.installed_revision
       AND image.updated_revision<=expected_revision
       AND accumulator.updated_revision<=expected_revision
       AND predecessor.semantic_status='sealed'
       AND EXISTS (SELECT 1 FROM groundloop_runtime_mode
                    WHERE singleton AND mode='m5_active')
       AND EXISTS (SELECT 1 FROM groundloop_epoch
                    WHERE epoch_id=selected_epoch_id
                      AND structural_status='committed'
                      AND semantic_status='complete'
                      AND evaluation_state='complete')
       AND EXISTS (SELECT 1 FROM groundloop_m5_runtime_epoch
                    WHERE epoch_id=selected_epoch_id
                      AND runtime_state='semantic_complete');
    IF NOT FOUND OR locked_epoch_revision<>expected_revision
       OR locked_runtime_revision<>expected_revision
       OR sealed_revision<>expected_revision+1
    THEN RAISE EXCEPTION 'persisted matching seal CAS/revision mismatch'; END IF;
    PERFORM groundloop_m5_matching_begin_promotion_context(
      'seal',selected_epoch_id,expected_revision,sealed_revision,selected_policy);
    PERFORM set_config('groundloop.m5_matching_mode','seal',true);
    PERFORM set_config('groundloop.m5_matching_epoch_id',selected_epoch_id::text,true);
    PERFORM set_config('groundloop.m5_matching_expected_revision',expected_revision::text,true);
    PERFORM set_config('groundloop.m5_matching_resulting_revision',sealed_revision::text,true);
    PERFORM set_config('groundloop.m5_matching_policy',selected_policy,true);
END;
$$;

CREATE FUNCTION groundloop_m5_authorize_persisted_matching_activation(
    expected_m4_head_epoch_id bigint, expected_head_epoch_revision bigint,
    selected_policy_version text
) RETURNS void LANGUAGE plpgsql SECURITY DEFINER SET search_path FROM CURRENT AS $$
DECLARE actual_head bigint; actual_revision bigint;
BEGIN
    IF to_regclass('pg_temp.groundloop_m5_matching_transition_context') IS NOT NULL
       OR to_regclass('pg_temp.groundloop_m5_matching_promotion_context') IS NOT NULL
       OR to_regclass('pg_temp.groundloop_m5_matching_promotion_journal') IS NOT NULL
       OR to_regclass('pg_temp.groundloop_m5_matching_promotion_expected') IS NOT NULL
       OR nullif(current_setting('groundloop.m5_matching_mode',true),'') IS NOT NULL
       OR nullif(current_setting('groundloop.m5_matching_context_oid',true),'') IS NOT NULL
       OR nullif(current_setting('groundloop.m5_matching_journal_oid',true),'') IS NOT NULL
       OR nullif(current_setting('groundloop.m5_matching_expected_oid',true),'') IS NOT NULL
    THEN RAISE EXCEPTION 'persisted matching authorization mode already selected'; END IF;
    PERFORM 1 FROM groundloop_runtime_mode WHERE singleton FOR UPDATE;
    PERFORM 1 FROM groundloop_m4_publication_head WHERE singleton FOR UPDATE;
    -- The caller's frozen relation locks protect the absent M5 singleton keys.
    PERFORM 1 FROM groundloop_epoch
      WHERE epoch_id=expected_m4_head_epoch_id FOR UPDATE;
    PERFORM 1 FROM groundloop_m5_runtime_epoch
      WHERE runtime_state NOT IN ('sealed','failed') FOR UPDATE;
    PERFORM 1 FROM groundloop_decision_policy
      WHERE policy_version=selected_policy_version FOR UPDATE;
    PERFORM 1 FROM groundloop_m5_schema_bundle
      WHERE bundle_id='m5-persisted-matching-schema-bundle-v1' FOR UPDATE;
    SELECT head.epoch_id, epoch.revision INTO STRICT actual_head, actual_revision
      FROM groundloop_runtime_mode mode
      JOIN groundloop_m4_publication_head head ON head.singleton
      JOIN groundloop_epoch epoch ON epoch.epoch_id=head.epoch_id
      JOIN groundloop_m5_schema_bundle ledger
        ON ledger.bundle_id='m5-persisted-matching-schema-bundle-v1'
       AND ledger.bundle_sha256 ~ '^[0-9a-f]{64}$'
       AND ledger.migration_sha256 ~ '^[0-9a-f]{64}$'
       AND ledger.oracle_sha256=
         'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855'
       AND ledger.prerequisite_sha256=
         '28a31f37c13cdaa2b89676e6279740a1f366e1acd16502c4fa722c2e0be21565'
      JOIN groundloop_decision_policy policy
        ON policy.policy_version=selected_policy_version
       AND policy.valid_from_epoch<=head.epoch_id
       AND (policy.valid_to_epoch IS NULL OR head.epoch_id<policy.valid_to_epoch)
     WHERE mode.singleton AND mode.mode='v1_only'
     ;
    IF actual_head<>expected_m4_head_epoch_id OR actual_revision<>expected_head_epoch_revision
       OR EXISTS (SELECT 1 FROM groundloop_m5_publication_head)
       OR EXISTS (SELECT 1 FROM groundloop_m5_activation)
       OR EXISTS (SELECT 1 FROM groundloop_m5_runtime_epoch WHERE runtime_state NOT IN ('sealed','failed'))
       OR EXISTS (SELECT 1 FROM groundloop_m5_matching_image_current)
       OR EXISTS (SELECT 1 FROM groundloop_m5_matching_image_working)
       OR EXISTS (SELECT 1 FROM groundloop_m5_matching_observation_current)
       OR EXISTS (SELECT 1 FROM groundloop_m5_matching_observation_working)
       OR EXISTS (SELECT 1 FROM groundloop_m5_matching_edge_current)
       OR EXISTS (SELECT 1 FROM groundloop_m5_matching_edge_working)
       OR EXISTS (SELECT 1 FROM groundloop_m5_matching_hash_mask_current)
       OR EXISTS (SELECT 1 FROM groundloop_m5_matching_hash_mask_working)
       OR EXISTS (SELECT 1 FROM groundloop_m5_matching_hall_current)
       OR EXISTS (SELECT 1 FROM groundloop_m5_matching_hall_working)
       OR EXISTS (SELECT 1 FROM groundloop_m5_matching_patch_artifact)
       OR EXISTS (SELECT 1 FROM groundloop_m5_matching_work_contribution)
       OR EXISTS (SELECT 1 FROM groundloop_m5_matching_work_accumulator)
    THEN RAISE EXCEPTION 'invalid persisted matching activation authorization'; END IF;
    PERFORM set_config('groundloop.m5_checked_transition','on',true);
    PERFORM groundloop_m5_matching_begin_promotion_context(
      'activation',actual_head,actual_revision,actual_revision,selected_policy_version);
    PERFORM set_config('groundloop.m5_matching_mode','activation',true);
    PERFORM set_config('groundloop.m5_matching_epoch_id',actual_head::text,true);
    PERFORM set_config('groundloop.m5_matching_expected_revision',actual_revision::text,true);
    PERFORM set_config('groundloop.m5_matching_resulting_revision',actual_revision::text,true);
    PERFORM set_config('groundloop.m5_matching_policy',selected_policy_version,true);
END;
$$;

DO $$ DECLARE relation_name text; BEGIN
  FOREACH relation_name IN ARRAY ARRAY[
    'groundloop_m5_matching_image_current','groundloop_m5_matching_image_working',
    'groundloop_m5_matching_observation_current','groundloop_m5_matching_observation_working',
    'groundloop_m5_matching_edge_current','groundloop_m5_matching_edge_working',
    'groundloop_m5_matching_hash_mask_current','groundloop_m5_matching_hash_mask_working',
    'groundloop_m5_matching_hall_current','groundloop_m5_matching_hall_working',
    'groundloop_m5_matching_patch_artifact','groundloop_m5_matching_work_contribution',
    'groundloop_m5_matching_work_accumulator'
  ] LOOP
    EXECUTE format('CREATE TRIGGER %I BEFORE INSERT OR UPDATE OR DELETE ON %I FOR EACH ROW EXECUTE FUNCTION groundloop_m5_matching_authorized_guard()', relation_name || '_guard', relation_name);
  END LOOP;
END $$;

REVOKE ALL ON FUNCTION groundloop_m5_matching_begin_transition_context(
  bigint,bigint,bigint,text,text) FROM PUBLIC;
REVOKE ALL ON FUNCTION groundloop_m5_matching_journal_change(
  text,text,jsonb,jsonb) FROM PUBLIC;
REVOKE ALL ON FUNCTION groundloop_m5_matching_begin_promotion_context(
  text,bigint,bigint,bigint,text) FROM PUBLIC;
REVOKE ALL ON FUNCTION groundloop_m5_matching_journal_promotion_change(
  text,text,jsonb,jsonb) FROM PUBLIC;

-- groundloop:m5-persisted-matching-group:deferred_validation
CREATE FUNCTION groundloop_m5_matching_capture_transition_anchor()
RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path FROM CURRENT AS $$
DECLARE started boolean;
BEGIN
  IF coalesce(current_setting('groundloop.m5_matching_mode',true),'')<>'transition' THEN
    RETURN NEW;
  END IF;
  IF to_regclass('pg_temp.groundloop_m5_matching_transition_context') IS NULL THEN
    RAISE EXCEPTION 'persisted matching runtime anchor lacks private context'; END IF;
  EXECUTE 'UPDATE pg_temp.groundloop_m5_matching_transition_context
              SET anchor_count=anchor_count+1,
                  runtime_first_old=CASE WHEN anchor_count=0 THEN $3 ELSE runtime_first_old END,
                  runtime_final_new=$4,
                  runtime_saw_insert=runtime_saw_insert OR $5=''INSERT'',
                  runtime_saw_update=runtime_saw_update OR $5=''UPDATE''
            WHERE backend_pid=$1 AND transaction_id=pg_current_xact_id()::text::bigint
              AND session_role=session_user AND epoch_id=$2
          RETURNING validation_started'
    INTO STRICT started USING pg_backend_pid(),NEW.epoch_id,
      CASE WHEN TG_OP='UPDATE' THEN to_jsonb(OLD) END,to_jsonb(NEW),TG_OP;
  IF started THEN RAISE EXCEPTION 'persisted matching anchor after validation'; END IF;
  RETURN NEW;
END;
$$;

CREATE FUNCTION groundloop_m5_matching_deferred_validate()
RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path FROM CURRENT AS $$
DECLARE selected_epoch bigint;
DECLARE selected_result bigint;
DECLARE contribution groundloop_m5_matching_work_contribution%ROWTYPE;
DECLARE patch groundloop_m5_matching_patch_artifact%ROWTYPE;
DECLARE accumulator groundloop_m5_matching_work_accumulator%ROWTYPE;
DECLARE counter_name text;
DECLARE expected_counter bigint;
DECLARE actual_counter bigint;
DECLARE relation_name text;
DECLARE anchor_count_value integer;
DECLARE validation_started_value boolean;
DECLARE validation_done_value boolean;
DECLARE context_source_kind text;
DECLARE journal_count bigint;
DECLARE expected_count bigint;
DECLARE child bytea;
DECLARE decoded_change jsonb;
DECLARE journal_row jsonb;
DECLARE key_value bytea;
DECLARE current_row_value jsonb;
DECLARE before_point_value jsonb;
DECLARE logical_outputs jsonb;
DECLARE logical_output jsonb;
DECLARE logical_kind text;
DECLARE logical_relation text;
DECLARE logical_fields jsonb;
DECLARE logical_digest text;
DECLARE logical_key_parts text[];
DECLARE status_count bigint;
DECLARE accumulator_journal jsonb;
DECLARE prior_counter bigint;
DECLARE contribution_counter bigint;
DECLARE contribution_journal jsonb;
DECLARE prior_status text;
DECLARE logical_patch_parsed jsonb;
DECLARE prior_digest text;
DECLARE prior_d25_revision bigint;
DECLARE prior_image_policy text;
DECLARE context_backend integer;
DECLARE context_xid bigint;
DECLARE context_role text;
DECLARE context_epoch_value bigint;
DECLARE context_expected bigint;
DECLARE context_result_value bigint;
DECLARE context_source_id text;
DECLARE runtime_first_old_value jsonb;
DECLARE runtime_final_new_value jsonb;
DECLARE runtime_saw_insert_value boolean;
DECLARE runtime_saw_update_value boolean;
DECLARE logical_change jsonb;
DECLARE effective_old jsonb;
DECLARE derived_additions bigint:=0;
DECLARE derived_removals bigint:=0;
DECLARE before_present boolean;
DECLARE after_present boolean;
DECLARE decoded_shapes jsonb;
DECLARE derived_crossings bigint:=0;
DECLARE derived_edge_keys bigint:=0;
DECLARE derived_mask_transitions bigint:=0;
DECLARE derived_masks_initialized bigint:=0;
DECLARE derived_zeta bigint:=0;
DECLARE derived_subset bigint:=0;
DECLARE derived_neighbours bigint:=0;
DECLARE derived_deficiency bigint:=0;
DECLARE old_mask integer;
DECLARE new_mask integer;
DECLARE group_r integer;
DECLARE subset_mask integer;
DECLARE hall_initialization boolean;
DECLARE old_refcount bigint;
DECLARE new_refcount bigint;
DECLARE derived_requirement_state_only bigint:=0;
DECLARE derived_group_state_only bigint:=0;
DECLARE derived_claim_state_only bigint:=0;
DECLARE initialized_hall_groups text[]:=ARRAY[]::text[];
DECLARE ordinary_mask_groups text[]:=ARRAY[]::text[];
BEGIN
  IF coalesce(current_setting('groundloop.m5_matching_mode',true),'')<>'transition' THEN
    IF to_regclass('pg_temp.groundloop_m5_matching_transition_context') IS NOT NULL
       OR nullif(current_setting('groundloop.m5_matching_context_oid',true),'') IS NOT NULL
       OR nullif(current_setting('groundloop.m5_matching_journal_oid',true),'') IS NOT NULL
       OR nullif(current_setting('groundloop.m5_matching_expected_oid',true),'') IS NOT NULL
    THEN RAISE EXCEPTION 'persisted matching transition mode/context escape'; END IF;
    RETURN NULL;
  END IF;
  IF to_regclass('pg_temp.groundloop_m5_matching_transition_context') IS NULL
     OR to_regclass('pg_temp.groundloop_m5_matching_change_journal') IS NULL
     OR to_regclass('pg_temp.groundloop_m5_matching_expected_changes') IS NULL
  THEN RAISE EXCEPTION 'persisted matching deferred validation lacks private context'; END IF;
  IF to_regclass('pg_temp.groundloop_m5_matching_transition_context')::oid::text<>
       current_setting('groundloop.m5_matching_context_oid',true)
     OR to_regclass('pg_temp.groundloop_m5_matching_change_journal')::oid::text<>
       current_setting('groundloop.m5_matching_journal_oid',true)
     OR to_regclass('pg_temp.groundloop_m5_matching_expected_changes')::oid::text<>
       current_setting('groundloop.m5_matching_expected_oid',true)
  THEN RAISE EXCEPTION 'persisted matching private context table was replaced'; END IF;
  selected_epoch := current_setting('groundloop.m5_matching_epoch_id')::bigint;
  selected_result := current_setting('groundloop.m5_matching_resulting_revision')::bigint;
  EXECUTE 'SELECT backend_pid,transaction_id,session_role,epoch_id,
                  expected_revision,resulting_revision,source_kind,source_id,
                  anchor_count,validation_started,validation_done,
                  runtime_first_old,runtime_final_new,
                  runtime_saw_insert,runtime_saw_update
             FROM pg_temp.groundloop_m5_matching_transition_context'
    INTO STRICT context_backend,context_xid,context_role,context_epoch_value,
      context_expected,context_result_value,context_source_kind,context_source_id,
      anchor_count_value,validation_started_value,validation_done_value,
      runtime_first_old_value,runtime_final_new_value,
      runtime_saw_insert_value,runtime_saw_update_value;
  EXECUTE 'SELECT count(*) FROM pg_temp.groundloop_m5_matching_transition_context'
    INTO journal_count;
  IF journal_count<>1 OR context_backend<>pg_backend_pid()
     OR context_xid<>pg_current_xact_id()::text::bigint
     OR context_role<>session_user OR context_epoch_value<>selected_epoch
     OR context_expected<>current_setting('groundloop.m5_matching_expected_revision')::bigint
     OR context_result_value<>selected_result
     OR context_source_kind<>current_setting('groundloop.m5_matching_source_kind')
     OR context_source_id<>current_setting('groundloop.m5_matching_source_id')
     OR current_setting('groundloop.m5_matching_mode')<>'transition'
  THEN RAISE EXCEPTION 'persisted matching private context/GUC mismatch'; END IF;
  IF validation_done_value THEN RETURN NULL; END IF;
  IF context_source_kind='structural_open' THEN
    IF anchor_count_value<>0 OR NOT EXISTS (
      SELECT 1 FROM groundloop_m5_runtime_epoch runtime
       WHERE runtime.epoch_id=selected_epoch AND runtime.revision=1
         AND runtime.structural_event_id=context_source_id
         AND runtime.runtime_state IN (
           'structural_committed','semantic_pending','semantic_complete')
         AND runtime.terminal_at IS NULL
         AND runtime.xmin=pg_current_xact_id()::xid)
    THEN RAISE EXCEPTION 'persisted matching structural runtime INSERT mismatch'; END IF;
  ELSE
    IF anchor_count_value<>1 OR runtime_saw_insert_value
       OR NOT runtime_saw_update_value OR runtime_first_old_value IS NULL
       OR runtime_final_new_value IS NULL
       OR (runtime_first_old_value->>'epoch_id')::bigint<>selected_epoch
       OR (runtime_final_new_value->>'epoch_id')::bigint<>selected_epoch
       OR (runtime_first_old_value->>'revision')::bigint<>context_expected
       OR (runtime_final_new_value->>'revision')::bigint<>selected_result
       OR runtime_first_old_value->>'structural_event_id'<>
          runtime_final_new_value->>'structural_event_id'
       OR runtime_first_old_value->>'candidate_policy_id'<>
          runtime_final_new_value->>'candidate_policy_id'
       OR runtime_first_old_value->>'candidate_policy_manifest_hash'<>
          runtime_final_new_value->>'candidate_policy_manifest_hash'
       OR runtime_first_old_value->>'requirement_registry_snapshot_digest'<>
          runtime_final_new_value->>'requirement_registry_snapshot_digest'
       OR runtime_first_old_value->>'active_chunk_snapshot_digest'<>
          runtime_final_new_value->>'active_chunk_snapshot_digest'
       OR runtime_first_old_value->>'expected_previous_published_epoch_id'<>
          runtime_final_new_value->>'expected_previous_published_epoch_id'
       OR runtime_first_old_value->>'requirement_root_set_hash'<>
          runtime_final_new_value->>'requirement_root_set_hash'
       OR runtime_first_old_value->>'runtime_state' IN ('sealed','failed')
       OR runtime_final_new_value->>'runtime_state' IN ('sealed','failed')
       OR runtime_first_old_value->'terminal_at'<>'null'::jsonb
       OR runtime_final_new_value->'terminal_at'<>'null'::jsonb
    THEN RAISE EXCEPTION 'persisted matching later runtime UPDATE mismatch'; END IF;
  END IF;
  IF validation_started_value THEN
    RAISE EXCEPTION 'persisted matching transition requires exactly one deferred anchor'; END IF;
  EXECUTE 'UPDATE pg_temp.groundloop_m5_matching_transition_context
              SET validation_started=true
            WHERE backend_pid=$1 AND transaction_id=pg_current_xact_id()::text::bigint
              AND session_role=session_user'
    USING pg_backend_pid();
  EXECUTE 'SELECT count(*) FROM pg_temp.groundloop_m5_matching_change_journal
            WHERE saw_delete OR (first_old IS NULL AND final_new IS NULL)'
    INTO journal_count;
  IF journal_count<>0 THEN
    RAISE EXCEPTION 'persisted matching transition cannot delete or cycle a working row'; END IF;
  SELECT c.* INTO contribution
  FROM groundloop_m5_matching_work_contribution c
  WHERE c.epoch_id=selected_epoch AND c.resulting_revision=selected_result
    AND c.source_kind=current_setting('groundloop.m5_matching_source_kind')
    AND c.source_id=current_setting('groundloop.m5_matching_source_id');
  IF NOT FOUND THEN
    RAISE EXCEPTION 'persisted matching transition lacks exact patch contribution';
  END IF;
  SELECT p.* INTO patch
  FROM groundloop_m5_matching_patch_artifact p
  WHERE p.patch_digest=contribution.patch_digest;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'persisted matching transition lacks exact patch artifact';
  END IF;
  IF ROW(patch.source_kind,patch.source_id,patch.source_identity_hash,
         patch.before_epoch_id,patch.before_revision,patch.resulting_epoch_id,
         patch.resulting_revision,patch.matching_work_digest) IS DISTINCT FROM
     ROW(contribution.source_kind,contribution.source_id,
         contribution.source_identity_hash,contribution.before_epoch_id,
         contribution.before_revision,contribution.epoch_id,
         contribution.resulting_revision,contribution.matching_work_digest)
  THEN RAISE EXCEPTION 'persisted matching patch/contribution identity mismatch'; END IF;
  IF patch.source_kind='structural_open' THEN
    IF NOT EXISTS (
      SELECT 1 FROM groundloop_epoch epoch
      JOIN groundloop_m5_runtime_epoch runtime USING(epoch_id)
      JOIN groundloop_m5_update typed_update USING(epoch_id)
      JOIN groundloop_m5_candidate_policy typed_policy
        ON typed_policy.candidate_policy_id=runtime.candidate_policy_id
      JOIN groundloop_m4_publication_head m4_head ON m4_head.singleton
      JOIN groundloop_m5_publication_head m5_head ON m5_head.singleton
      JOIN groundloop_m5_matching_image_current current_image
        ON current_image.singleton
      JOIN groundloop_decision_policy current_policy
        ON current_policy.policy_version=current_image.decision_policy_version
      JOIN groundloop_epoch predecessor
        ON predecessor.epoch_id=typed_update.previous_published_epoch_id
      WHERE epoch.epoch_id=selected_epoch
        AND epoch.event_id=patch.source_id
        AND epoch.payload_hash=patch.source_identity_hash
        AND runtime.structural_event_id=patch.source_id
        AND runtime.revision=1
        AND typed_update.previous_published_epoch_id=patch.before_epoch_id
        AND typed_update.decision_policy_version=patch.decision_policy_version
        AND typed_policy.decision_policy_version=patch.decision_policy_version
        AND m4_head.epoch_id=patch.before_epoch_id
        AND m5_head.epoch_id=patch.before_epoch_id
        AND m5_head.sealed_revision=patch.before_revision
        AND current_image.installed_epoch_id=patch.before_epoch_id
        AND current_image.installed_revision=patch.before_revision
        AND current_policy.valid_from_epoch<=patch.before_epoch_id
        AND (current_policy.valid_to_epoch IS NULL
             OR current_policy.valid_to_epoch>patch.before_epoch_id)
        AND predecessor.revision=patch.before_revision
        AND predecessor.structural_status='committed'
        AND predecessor.semantic_status='sealed'
        AND predecessor.evaluation_state='complete'
        AND predecessor.sealed_at IS NOT NULL
        AND NOT EXISTS (
          SELECT 1 FROM groundloop_m4_update direct_update
          JOIN groundloop_candidate_policy direct_policy
            ON direct_policy.candidate_policy_id=direct_update.candidate_policy_id
          WHERE direct_update.epoch_id=selected_epoch
            AND direct_policy.decision_policy_version<>
                patch.decision_policy_version))
    THEN RAISE EXCEPTION 'persisted matching structural source authority mismatch'; END IF;
  ELSIF patch.source_kind='requirement_completion' THEN
    IF NOT EXISTS (
      SELECT 1
      FROM groundloop_m5_attempt_result_artifact artifact
      JOIN groundloop_m5_job_attempt attempt
        ON attempt.attempt_id=artifact.attempt_id
       AND attempt.logical_job_id=artifact.logical_job_id
      JOIN groundloop_m5_semantic_job job
        ON job.logical_job_id=artifact.logical_job_id
       AND job.epoch_id=artifact.job_epoch_id
      JOIN groundloop_m5_requirement_verifier_execution execution
        ON execution.logical_job_id=artifact.logical_job_id
       AND execution.attempt_id=artifact.attempt_id
       AND execution.artifact_id=artifact.result_artifact_id
       AND execution.artifact_hash=artifact.result_artifact_hash
      JOIN groundloop_m5_requirement_verifier_artifact verifier_artifact
        ON verifier_artifact.artifact_id=execution.artifact_id
       AND verifier_artifact.artifact_hash=execution.artifact_hash
       AND verifier_artifact.semantic_pair_digest=job.semantic_pair_digest
      WHERE artifact.attempt_id=patch.source_id
        AND artifact.attempt_result_artifact_hash=patch.source_identity_hash
        AND artifact.job_epoch_id=selected_epoch
        AND artifact.disposition='verifier_completed_active'
        AND job.job_kind='verify_requirement_pair'
        AND attempt.attempt_state='completed'
        AND attempt.attempt_output_digest=artifact.attempt_output_digest
        AND attempt.execution_spec_hash=artifact.execution_spec_hash
        AND job.payload_hash=artifact.payload_hash
        AND job.execution_spec_hash=artifact.execution_spec_hash
        AND job.job_state=artifact.job_state_after
        AND job.result_artifact_id=artifact.result_artifact_id
        AND job.result_artifact_hash=artifact.result_artifact_hash
        AND job.completed_revision=selected_result
        AND verifier_artifact.execution_spec_hash=job.execution_spec_hash
        AND verifier_artifact.subject_kind=job.subject_kind
        AND verifier_artifact.subject_id=job.subject_id
        AND verifier_artifact.chunk_version_id=job.chunk_version_id
        AND execution.pair_input_hash=verifier_artifact.pair_input_hash
        AND execution.produced_epoch_id=selected_epoch)
    THEN RAISE EXCEPTION 'persisted matching requirement source authority mismatch'; END IF;
  ELSE
    IF NOT EXISTS (
      SELECT 1 FROM groundloop_m4_evaluation_counter_transition transition
       WHERE transition.epoch_id=selected_epoch
         AND transition.transition_id=patch.source_id
         AND transition.payload_hash=patch.source_identity_hash
         AND transition.transition_kind='delta'
         AND transition.from_revision=patch.before_revision
         AND transition.to_revision=selected_result)
    THEN RAISE EXCEPTION 'persisted matching direct source authority mismatch'; END IF;
  END IF;
  key_value:=groundloop_m5_matching_journal_key(
    'groundloop_m5_matching_patch_artifact',ARRAY[patch.patch_digest::text]);
  EXECUTE 'INSERT INTO pg_temp.groundloop_m5_matching_expected_changes
    (relation_name,key_preimage) VALUES ($1,$2)'
    USING 'groundloop_m5_matching_patch_artifact',key_value;
  EXECUTE 'SELECT to_jsonb(j) FROM pg_temp.groundloop_m5_matching_change_journal j
            WHERE relation_name=''groundloop_m5_matching_patch_artifact''
              AND key_preimage=$1' INTO journal_row USING key_value;
  IF journal_row IS NULL OR journal_row->>'first_operation'<>'INSERT'
     OR journal_row->>'last_operation'<>'INSERT'
     OR (journal_row->>'mutation_count')::integer<>1
     OR journal_row->'final_new'->>'patch_digest'<>patch.patch_digest
     OR decode(substr(journal_row->'final_new'->>'canonical_patch_preimage',3),'hex')<>
        patch.canonical_patch_preimage
  THEN RAISE EXCEPTION 'persisted matching patch artifact journal mismatch'; END IF;
  key_value:=groundloop_m5_matching_journal_key(
    'groundloop_m5_matching_work_contribution',ARRAY[selected_epoch::text,
      contribution.source_kind,contribution.source_id,selected_result::text]);
  EXECUTE 'INSERT INTO pg_temp.groundloop_m5_matching_expected_changes
    (relation_name,key_preimage) VALUES ($1,$2)'
    USING 'groundloop_m5_matching_work_contribution',key_value;
  EXECUTE 'SELECT to_jsonb(j) FROM pg_temp.groundloop_m5_matching_change_journal j
            WHERE relation_name=''groundloop_m5_matching_work_contribution''
              AND key_preimage=$1' INTO journal_row USING key_value;
  IF journal_row IS NULL OR journal_row->>'first_operation'<>'INSERT'
     OR journal_row->>'last_operation'<>'INSERT'
     OR (journal_row->>'mutation_count')::integer<>1
     OR journal_row->'final_new'->>'contribution_digest'<>contribution.contribution_digest
     OR journal_row->'final_new'->>'patch_digest'<>contribution.patch_digest
     OR journal_row->'final_new'->>'matching_work_digest'<>contribution.matching_work_digest
  THEN RAISE EXCEPTION 'persisted matching contribution journal mismatch'; END IF;
  contribution_journal:=journal_row;
  IF NOT EXISTS (
       SELECT 1 FROM groundloop_m5_matching_image_working image
       JOIN groundloop_m5_runtime_epoch runtime USING(epoch_id)
       WHERE image.epoch_id=selected_epoch
         AND image.updated_revision=selected_result
         AND runtime.revision=selected_result)
  THEN RAISE EXCEPTION 'persisted matching transition lacks exact final image/revision'; END IF;
  key_value:=groundloop_m5_matching_journal_key(
    'groundloop_m5_matching_image_working',ARRAY[selected_epoch::text]);
  EXECUTE 'INSERT INTO pg_temp.groundloop_m5_matching_expected_changes
    (relation_name,key_preimage) VALUES ($1,$2)'
    USING 'groundloop_m5_matching_image_working',key_value;
  EXECUTE 'SELECT to_jsonb(j) FROM pg_temp.groundloop_m5_matching_change_journal j
            WHERE relation_name=''groundloop_m5_matching_image_working''
              AND key_preimage=$1' INTO journal_row USING key_value;
  IF journal_row IS NULL
     OR (journal_row->'final_new'->>'epoch_id')::bigint<>selected_epoch
     OR (journal_row->'final_new'->>'updated_revision')::bigint<>selected_result
     OR journal_row->'final_new'->>'decision_policy_version'<>patch.decision_policy_version
  THEN RAISE EXCEPTION 'persisted matching working image journal mismatch'; END IF;
  IF patch.source_kind='structural_open' THEN
    IF journal_row->>'first_operation'<>'INSERT'
       OR journal_row->>'last_operation'<>'INSERT'
       OR (journal_row->>'mutation_count')::integer<>1
       OR journal_row->'first_old'<>'null'::jsonb
       OR NOT (journal_row->>'saw_insert')::boolean
       OR (journal_row->>'saw_update')::boolean
       OR (journal_row->>'saw_delete')::boolean
       OR (journal_row->'final_new'->>'base_epoch_id')::bigint<>patch.before_epoch_id
       OR (journal_row->'final_new'->>'base_revision')::bigint<>patch.before_revision
    THEN RAISE EXCEPTION 'persisted matching structural image requires one INSERT'; END IF;
  ELSE
    SELECT prior.resulting_revision,prior_patch.decision_policy_version
      INTO STRICT prior_d25_revision,prior_image_policy
      FROM groundloop_m5_matching_work_contribution prior
      JOIN groundloop_m5_matching_patch_artifact prior_patch
        ON prior_patch.patch_digest=prior.patch_digest
     WHERE prior.epoch_id=selected_epoch
       AND prior.resulting_revision=(SELECT max(earlier.resulting_revision)
         FROM groundloop_m5_matching_work_contribution earlier
         WHERE earlier.epoch_id=selected_epoch
           AND earlier.resulting_revision<selected_result);
    IF journal_row->>'first_operation'<>'UPDATE'
       OR journal_row->>'last_operation'<>'UPDATE'
       OR (journal_row->>'mutation_count')::integer<>1
       OR (journal_row->>'saw_insert')::boolean
       OR NOT (journal_row->>'saw_update')::boolean
       OR (journal_row->>'saw_delete')::boolean
       OR (journal_row->'first_old'->>'epoch_id')::bigint<>selected_epoch
       OR journal_row->'final_new'->>'base_epoch_id'<>
          journal_row->'first_old'->>'base_epoch_id'
       OR journal_row->'final_new'->>'base_revision'<>
          journal_row->'first_old'->>'base_revision'
       OR journal_row->'final_new'->>'decision_policy_version'<>
          journal_row->'first_old'->>'decision_policy_version'
       OR journal_row->'first_old'->>'decision_policy_version'<>prior_image_policy
       OR (journal_row->'first_old'->>'updated_revision')::bigint<>prior_d25_revision
    THEN RAISE EXCEPTION 'persisted matching later image OLD/update mismatch'; END IF;
  END IF;
  SELECT * INTO STRICT accumulator FROM groundloop_m5_matching_work_accumulator
  WHERE epoch_id=selected_epoch;
  key_value:=groundloop_m5_matching_journal_key(
    'groundloop_m5_matching_work_accumulator',ARRAY[selected_epoch::text]);
  EXECUTE 'INSERT INTO pg_temp.groundloop_m5_matching_expected_changes
    (relation_name,key_preimage) VALUES ($1,$2)'
    USING 'groundloop_m5_matching_work_accumulator',key_value;
  EXECUTE 'SELECT to_jsonb(j) FROM pg_temp.groundloop_m5_matching_change_journal j
            WHERE relation_name=''groundloop_m5_matching_work_accumulator''
              AND key_preimage=$1' INTO accumulator_journal USING key_value;
  IF accumulator_journal IS NULL
     OR (accumulator_journal->'final_new'->>'epoch_id')::bigint<>accumulator.epoch_id
     OR accumulator_journal->'final_new'->>'matching_work_digest'<>accumulator.matching_work_digest
     OR (accumulator_journal->'final_new'->>'updated_revision')::bigint<>accumulator.updated_revision THEN
    RAISE EXCEPTION 'persisted matching accumulator journal/final row mismatch'; END IF;
  IF patch.source_kind='structural_open' THEN
    IF accumulator_journal->>'first_operation'<>'INSERT'
       OR accumulator_journal->>'last_operation'<>'INSERT'
       OR (accumulator_journal->>'mutation_count')::integer<>1
       OR accumulator_journal->'first_old'<>'null'::jsonb
       OR NOT (accumulator_journal->>'saw_insert')::boolean
       OR (accumulator_journal->>'saw_update')::boolean
       OR (accumulator_journal->>'saw_delete')::boolean
    THEN RAISE EXCEPTION 'persisted matching structural accumulator requires one INSERT'; END IF;
  ELSE
    IF accumulator_journal->>'first_operation'<>'UPDATE'
       OR accumulator_journal->>'last_operation'<>'UPDATE'
       OR (accumulator_journal->>'mutation_count')::integer<>1
       OR accumulator_journal->'first_old'='null'::jsonb
       OR (accumulator_journal->'first_old'->>'epoch_id')::bigint<>selected_epoch
       OR (accumulator_journal->'first_old'->>'updated_revision')::bigint<>
          prior_d25_revision
       OR accumulator_journal->'first_old'->>'matching_work_digest'<>
          groundloop_m5_matching_work_digest(
            groundloop_m5_matching_work_values(accumulator_journal->'first_old'))
       OR (accumulator_journal->>'saw_insert')::boolean
       OR NOT (accumulator_journal->>'saw_update')::boolean
       OR (accumulator_journal->>'saw_delete')::boolean
    THEN RAISE EXCEPTION 'persisted matching later accumulator requires one UPDATE'; END IF;
  END IF;
  FOREACH counter_name IN ARRAY ARRAY[
    'contribution_additions','contribution_removals',
    'requirement_observation_changes_processed','policy_candidate_observations',
    'ordered_policy_range_probes','ordered_index_operations','canonical_sort_items',
    'edge_refcount_keys_updated','distinct_edge_crossings','hash_mask_transitions',
    'hash_masks_initialized','hall_zeta_additions','hall_subset_entries_examined',
    'hall_neighbor_entries_changed','hall_deficiency_entries_examined',
    'certificate_repairs','certificate_reconstructions','policy_rebindings',
    'representative_hashes_read','representative_observations_read',
    'augmenting_searches','augmenting_requirement_visits','augmenting_edge_visits',
    'certificate_digest_input_bytes','group_local_state_operations','groups_touched',
    'claims_touched','answers_touched','claim_status_changes','answer_status_changes',
    'output_bytes','requirement_state_only_changes','group_state_only_changes',
    'claim_state_only_changes','group_certificate_only_changes',
    'claim_certificate_only_changes','public_status_deltas'
  ] LOOP
    EXECUTE format(
      'SELECT coalesce(sum((to_jsonb(c)->>%L)::bigint),0) FROM groundloop_m5_matching_work_contribution c WHERE epoch_id=$1',
      counter_name) INTO expected_counter USING selected_epoch;
    actual_counter := (to_jsonb(accumulator)->>counter_name)::bigint;
    IF actual_counter<>expected_counter THEN
      RAISE EXCEPTION 'persisted matching accumulator sum mismatch for %',counter_name;
    END IF;
    prior_counter:=coalesce((accumulator_journal->'first_old'->>counter_name)::bigint,0);
    contribution_counter:=(to_jsonb(contribution)->>counter_name)::bigint;
    IF (contribution_journal->'final_new'->>counter_name)::bigint<>
       contribution_counter THEN
      RAISE EXCEPTION 'persisted matching contribution journal counter mismatch for %',counter_name;
    END IF;
    IF actual_counter<>prior_counter+contribution_counter THEN
      RAISE EXCEPTION 'persisted matching accumulator OLD plus contribution mismatch for %',counter_name;
    END IF;
  END LOOP;
  IF accumulator.updated_revision<>(
      SELECT max(resulting_revision) FROM groundloop_m5_matching_work_contribution
      WHERE epoch_id=selected_epoch)
  THEN RAISE EXCEPTION 'persisted matching accumulator revision mismatch'; END IF;
  logical_outputs:=groundloop_m5_matching_validate_logical_output(
    patch.logical_output_preimage);
  decoded_shapes:=groundloop_m5_matching_validate_group_shapes(
    patch.group_shape_set_preimage);
  logical_patch_parsed:=groundloop_m5_matching_parse_typed_preimage(
    patch.logical_overlay_patch_preimage,'m5-persisted-logical-overlay-patch-v1');
  FOR child IN SELECT unnest(patch.observation_change_preimages) LOOP
    decoded_change:=groundloop_m5_matching_validate_change(child,'observation');
    before_present:=CASE
      WHEN decoded_change->'before'='null'::jsonb THEN false
      WHEN decoded_change->'before'->>'layer'='current' THEN true
      ELSE (decoded_change->'before'->>'present')::boolean END;
    after_present:=(decoded_change->'after'->>'present')::boolean;
    derived_additions:=derived_additions+
      CASE WHEN NOT before_present AND after_present THEN 1 ELSE 0 END;
    derived_removals:=derived_removals+
      CASE WHEN before_present AND NOT after_present THEN 1 ELSE 0 END;
  END LOOP;
  FOR child IN SELECT unnest(patch.hall_change_preimages) LOOP
    decoded_change:=groundloop_m5_matching_validate_change(child,'hall');
    IF decoded_change->'before'='null'::jsonb
       AND decoded_change->'after'->>'requirement_count' IS NOT NULL THEN
      initialized_hall_groups:=array_append(
        initialized_hall_groups,decoded_change->>'outer_one');
    END IF;
  END LOOP;
  FOR child IN SELECT unnest(patch.mask_change_preimages) LOOP
    decoded_change:=groundloop_m5_matching_validate_change(child,'mask');
    old_mask:=CASE WHEN decoded_change->'before'='null'::jsonb THEN 0
      ELSE (decoded_change->'before'->>'mask')::integer END;
    new_mask:=(decoded_change->'after'->>'mask')::integer;
    IF decoded_change->>'outer_one'=ANY(initialized_hall_groups) THEN
      IF new_mask>0 THEN
        derived_masks_initialized:=derived_masks_initialized+1;
      END IF;
    ELSIF old_mask<>new_mask THEN
      derived_mask_transitions:=derived_mask_transitions+1;
      ordinary_mask_groups:=array_append(
        ordinary_mask_groups,decoded_change->>'outer_one');
      derived_crossings:=derived_crossings+
        bit_count((old_mask # new_mask)::bit(64));
      SELECT (shape.value->>'requirement_count')::integer INTO STRICT group_r
        FROM jsonb_array_elements(decoded_shapes) shape(value)
       WHERE shape.value->>'group_id'=decoded_change->>'outer_one';
      derived_subset:=derived_subset+((1::bigint<<group_r)-1);
      derived_deficiency:=derived_deficiency+((1::bigint<<group_r)-1);
      FOR subset_mask IN 1..((1<<group_r)-1) LOOP
        IF ((old_mask & subset_mask)<>0)<>( (new_mask & subset_mask)<>0) THEN
          derived_neighbours:=derived_neighbours+1;
        END IF;
      END LOOP;
    END IF;
  END LOOP;
  FOR child IN SELECT unnest(patch.edge_change_preimages) LOOP
    decoded_change:=groundloop_m5_matching_validate_change(child,'edge');
    old_refcount:=CASE WHEN decoded_change->'before'='null'::jsonb THEN 0
      WHEN decoded_change->'before'->>'layer'='current' THEN
        (decoded_change->'before'->'node'->'children'->5->>'value')::bigint
      ELSE (decoded_change->'before'->'node'->'children'->6->>'value')::bigint END;
    new_refcount:=(decoded_change->'after'->'node'->'children'->6->>'value')::bigint;
    IF old_refcount<>new_refcount THEN
      derived_edge_keys:=derived_edge_keys+1;
    END IF;
  END LOOP;
  FOR child IN SELECT unnest(patch.hall_change_preimages) LOOP
    decoded_change:=groundloop_m5_matching_validate_change(child,'hall');
    hall_initialization:=decoded_change->'before'='null'::jsonb
      AND decoded_change->'after'->>'requirement_count' IS NOT NULL;
    IF hall_initialization THEN
      group_r:=(decoded_change->'after'->>'requirement_count')::integer;
      derived_zeta:=derived_zeta+group_r*(1::bigint<<(group_r-1));
      derived_subset:=derived_subset+((1::bigint<<group_r)-1);
      derived_deficiency:=derived_deficiency+((1::bigint<<group_r)-1);
      SELECT count(*) INTO expected_count
        FROM unnest(patch.mask_change_preimages) mask_child
       WHERE (groundloop_m5_matching_validate_change(mask_child,'mask')->>'outer_one')=
             decoded_change->>'outer_one'
         AND (groundloop_m5_matching_validate_change(mask_child,'mask')->'after'->>'mask')::integer>0;
      IF expected_count<>(decoded_change->'after'->'node'->'children'->10->>'value')::bigint THEN
        RAISE EXCEPTION 'persisted matching Hall initialization mask cardinality mismatch'; END IF;
    END IF;
  END LOOP;
  IF contribution.output_bytes<>octet_length(patch.logical_output_preimage)
     OR contribution.contribution_additions<>derived_additions
     OR contribution.contribution_removals<>derived_removals
     OR contribution.edge_refcount_keys_updated<>derived_edge_keys
     OR contribution.hash_mask_transitions<>derived_mask_transitions
     OR contribution.distinct_edge_crossings<>derived_crossings
     OR contribution.hash_masks_initialized<>derived_masks_initialized
     OR contribution.hall_zeta_additions<>derived_zeta
     OR contribution.hall_subset_entries_examined<>derived_subset
     OR contribution.hall_neighbor_entries_changed<>derived_neighbours
     OR contribution.hall_deficiency_entries_examined<>derived_deficiency
     OR contribution.public_status_deltas<>(SELECT count(*) FROM
          jsonb_array_elements(logical_outputs) item(value)
          WHERE item.value->>'kind'='status_delta')
     OR contribution.claim_status_changes<>(SELECT count(*) FROM
          jsonb_array_elements(logical_outputs) item(value)
          WHERE item.value->>'kind'='status_delta'
            AND item.value->'after'->'fields'->1->'value'->>'value'='claim')
     OR contribution.answer_status_changes<>(SELECT count(*) FROM
          jsonb_array_elements(logical_outputs) item(value)
          WHERE item.value->>'kind'='status_delta'
            AND item.value->'after'->'fields'->1->'value'->>'value'='answer')
  THEN RAISE EXCEPTION 'persisted matching derivable work counter mismatch'; END IF;
  FOR logical_output IN SELECT entry.value
    FROM jsonb_array_elements(logical_outputs) entry(value)
    WHERE entry.value->>'kind' IN (
      'requirement_state','group_state','claim_state','answer_state',
      'group_binding','claim_binding')
  LOOP
    logical_kind:=logical_output->>'kind';
    logical_relation:=CASE logical_kind
      WHEN 'requirement_state' THEN 'groundloop_m5_working_requirement_state'
      WHEN 'group_state' THEN 'groundloop_m5_working_group_state'
      WHEN 'claim_state' THEN 'groundloop_m5_working_claim_state'
      WHEN 'answer_state' THEN 'groundloop_m5_working_answer_state'
      WHEN 'group_binding' THEN 'groundloop_m5_working_group_certificate_binding'
      ELSE 'groundloop_m5_working_claim_certificate_binding' END;
    IF logical_kind IN ('requirement_state','group_state','claim_state','answer_state') THEN
      SELECT change.value INTO STRICT logical_change FROM jsonb_array_elements(
        logical_patch_parsed->'children'->0->'children') change(value)
       WHERE change.value->'children'->0->>'value'=logical_kind
         AND change.value->'children'->1->>'value'=logical_output->>'object_id';
    END IF;
    IF logical_output->'after'->>'tag'='none' THEN
      IF logical_kind NOT IN ('requirement_state','group_state')
         OR patch.source_kind<>'structural_open'
         OR logical_change->'children'->3->>'tag'<>'null'
      THEN RAISE EXCEPTION 'persisted matching logical state removal is forbidden'; END IF;
      IF logical_kind='requirement_state' THEN
        SELECT to_jsonb(state) INTO effective_old
          FROM groundloop_m5_published_requirement_state state
         WHERE state.requirement_version_id=logical_output->>'object_id'
           AND state.valid_from_epoch<=patch.before_epoch_id
           AND (state.valid_to_epoch IS NULL OR state.valid_to_epoch>patch.before_epoch_id);
        IF EXISTS (SELECT 1 FROM groundloop_m5_effective_requirement_version effective
                    WHERE effective.epoch_id=selected_epoch
                      AND effective.requirement_version_id=logical_output->>'object_id')
           OR NOT EXISTS (SELECT 1 FROM groundloop_m5_group_deactivation deactivation
             JOIN groundloop_epoch epoch USING(epoch_id)
             JOIN groundloop_m5_update update_row USING(epoch_id)
             JOIN groundloop_m5_requirement_version requirement
               ON requirement.group_version_id=deactivation.group_version_id
            WHERE deactivation.epoch_id=selected_epoch
              AND deactivation.event_id=epoch.event_id
              AND deactivation.event_id=patch.source_id
              AND ((update_row.update_kind='replace_group'
                    AND deactivation.action='REPLACE')
                OR (update_row.update_kind='retire_group'
                    AND deactivation.action='RETIRE'))
              AND requirement.requirement_version_id=logical_output->>'object_id')
        THEN RAISE EXCEPTION 'persisted matching requirement removal lacks exact deactivation'; END IF;
      ELSE
        SELECT to_jsonb(state) INTO effective_old
          FROM groundloop_m5_published_group_state state
         WHERE state.group_version_id=logical_output->>'object_id'
           AND state.valid_from_epoch<=patch.before_epoch_id
           AND (state.valid_to_epoch IS NULL OR state.valid_to_epoch>patch.before_epoch_id);
        IF EXISTS (SELECT 1 FROM groundloop_m5_effective_group_version effective
                    WHERE effective.epoch_id=selected_epoch
                      AND effective.group_version_id=logical_output->>'object_id')
           OR NOT EXISTS (SELECT 1 FROM groundloop_m5_group_deactivation deactivation
             JOIN groundloop_epoch epoch USING(epoch_id)
             JOIN groundloop_m5_update update_row USING(epoch_id)
            WHERE deactivation.epoch_id=selected_epoch
              AND deactivation.event_id=epoch.event_id
              AND deactivation.event_id=patch.source_id
              AND ((update_row.update_kind='replace_group'
                    AND deactivation.action='REPLACE')
                OR (update_row.update_kind='retire_group'
                    AND deactivation.action='RETIRE'))
              AND deactivation.group_version_id=logical_output->>'object_id')
        THEN RAISE EXCEPTION 'persisted matching group removal lacks exact deactivation'; END IF;
      END IF;
      IF effective_old IS NULL
         OR logical_change->'children'->2->>'tag'<>'sha256'
         OR logical_change->'children'->2->>'value'<>
              groundloop_m5_matching_state_digest(logical_kind,effective_old)
      THEN RAISE EXCEPTION 'persisted matching removed state before digest mismatch'; END IF;
      EXECUTE format('SELECT EXISTS (SELECT 1 FROM %I WHERE epoch_id=$1 AND %I=$2)',
        logical_relation,CASE logical_kind WHEN 'requirement_state'
          THEN 'requirement_version_id' ELSE 'group_version_id' END)
        INTO before_present USING selected_epoch,logical_output->>'object_id';
      key_value:=groundloop_m5_matching_journal_key(logical_relation,
        ARRAY[selected_epoch::text,logical_output->>'object_id']);
      EXECUTE 'SELECT count(*) FROM pg_temp.groundloop_m5_matching_change_journal
                WHERE relation_name=$1 AND key_preimage=$2'
        INTO journal_count USING logical_relation,key_value;
      IF before_present OR journal_count<>0 THEN
        RAISE EXCEPTION 'persisted matching removed state emitted a working mutation'; END IF;
      CONTINUE;
    END IF;
    logical_fields:=logical_output->'after'->'fields';
    logical_key_parts:=CASE logical_kind
      WHEN 'group_binding' THEN ARRAY[selected_epoch::text,logical_output->>'object_id',
        logical_fields->2->'value'->>'value']
      WHEN 'claim_binding' THEN ARRAY[selected_epoch::text,logical_output->>'object_id',
        logical_fields->2->'value'->>'value']
      ELSE ARRAY[selected_epoch::text,logical_output->>'object_id'] END;
    key_value:=groundloop_m5_matching_journal_key(logical_relation,logical_key_parts);
    EXECUTE 'INSERT INTO pg_temp.groundloop_m5_matching_expected_changes
      (relation_name,key_preimage) VALUES ($1,$2)' USING logical_relation,key_value;
    EXECUTE 'SELECT to_jsonb(j) FROM pg_temp.groundloop_m5_matching_change_journal j
              WHERE relation_name=$1 AND key_preimage=$2'
      INTO journal_row USING logical_relation,key_value;
    IF logical_kind IN ('requirement_state','group_state','claim_state','answer_state') THEN
      effective_old:=journal_row->'first_old';
      IF effective_old='null'::jsonb THEN
        IF logical_kind='requirement_state' THEN
          SELECT to_jsonb(state) INTO effective_old FROM groundloop_m5_published_requirement_state state
           WHERE state.requirement_version_id=logical_output->>'object_id'
             AND state.valid_from_epoch<=patch.before_epoch_id
             AND (state.valid_to_epoch IS NULL OR state.valid_to_epoch>patch.before_epoch_id);
        ELSIF logical_kind='group_state' THEN
          SELECT to_jsonb(state) INTO effective_old FROM groundloop_m5_published_group_state state
           WHERE state.group_version_id=logical_output->>'object_id'
             AND state.valid_from_epoch<=patch.before_epoch_id
             AND (state.valid_to_epoch IS NULL OR state.valid_to_epoch>patch.before_epoch_id);
        ELSIF logical_kind='claim_state' THEN
          SELECT to_jsonb(state) INTO effective_old FROM groundloop_m5_published_claim_state state
           WHERE state.claim_id=logical_output->>'object_id'
             AND state.valid_from_epoch<=patch.before_epoch_id
             AND (state.valid_to_epoch IS NULL OR state.valid_to_epoch>patch.before_epoch_id);
        ELSE
          SELECT to_jsonb(state) INTO effective_old FROM groundloop_m5_published_answer_state state
           WHERE state.answer_version_id=logical_output->>'object_id'
             AND state.valid_from_epoch<=patch.before_epoch_id
             AND (state.valid_to_epoch IS NULL OR state.valid_to_epoch>patch.before_epoch_id);
        END IF;
      END IF;
      IF (logical_change->'children'->2->>'tag'='null')<>(effective_old IS NULL)
         OR (effective_old IS NOT NULL AND
           logical_change->'children'->2->>'value'<>
             groundloop_m5_matching_state_digest(logical_kind,effective_old))
         OR logical_change->'children'->3->>'value'<>
           groundloop_m5_matching_state_digest(logical_kind,journal_row->'final_new')
      THEN RAISE EXCEPTION 'persisted matching logical state before/after digest mismatch'; END IF;
      IF logical_change->'children'->2->>'tag'='sha256'
         AND logical_change->'children'->3->>'tag'='sha256' THEN
        IF logical_kind='requirement_state'
           AND (effective_old->>'satisfied')::boolean=
               (journal_row->'final_new'->>'satisfied')::boolean THEN
          derived_requirement_state_only:=derived_requirement_state_only+1;
        ELSIF logical_kind='group_state'
           AND (effective_old->>'complete')::boolean=
               (journal_row->'final_new'->>'complete')::boolean THEN
          derived_group_state_only:=derived_group_state_only+1;
        ELSIF logical_kind='claim_state'
           AND effective_old->>'status'=journal_row->'final_new'->>'status' THEN
          derived_claim_state_only:=derived_claim_state_only+1;
        END IF;
      END IF;
    END IF;
    IF journal_row IS NULL OR NOT groundloop_m5_matching_logical_row_matches(
      logical_kind,journal_row->'final_new',logical_output,selected_epoch,
      selected_result,patch.decision_policy_version)
      OR groundloop_m5_matching_logical_row_preimage(
           logical_kind,journal_row->'final_new')<>
         groundloop_m5_matching_reencode_logical(logical_output->'after')
    THEN RAISE EXCEPTION 'persisted matching logical journal/output mismatch for %',logical_kind; END IF;
    IF logical_kind IN ('group_binding','claim_binding') THEN
      IF logical_fields->3->'value'->>'tag'='none' THEN
        IF journal_row->>'first_operation'<>'INSERT'
           OR journal_row->>'last_operation'<>'INSERT'
           OR (journal_row->>'mutation_count')::integer<>1
           OR NOT (journal_row->>'saw_insert')::boolean
           OR (journal_row->>'saw_update')::boolean
           OR (journal_row->>'saw_delete')::boolean
        THEN RAISE EXCEPTION 'persisted matching binding open requires one INSERT'; END IF;
      ELSE
        IF journal_row->>'first_operation'<>'UPDATE'
           OR journal_row->>'last_operation'<>'UPDATE'
           OR (journal_row->>'mutation_count')::integer<>1
           OR (journal_row->>'saw_insert')::boolean
           OR NOT (journal_row->>'saw_update')::boolean
           OR (journal_row->>'saw_delete')::boolean
        THEN RAISE EXCEPTION 'persisted matching binding close requires one UPDATE'; END IF;
      END IF;
    ELSIF journal_row->'first_old'='null'::jsonb THEN
      IF journal_row->>'first_operation'<>'INSERT'
         OR journal_row->>'last_operation'<>'INSERT'
         OR (journal_row->>'mutation_count')::integer<>1
         OR NOT (journal_row->>'saw_insert')::boolean
         OR (journal_row->>'saw_update')::boolean
         OR (journal_row->>'saw_delete')::boolean
      THEN RAISE EXCEPTION 'persisted matching logical state requires one INSERT'; END IF;
    ELSE
      IF journal_row->>'first_operation'<>'UPDATE'
         OR journal_row->>'last_operation'<>'UPDATE'
         OR (journal_row->>'mutation_count')::integer<>1
         OR (journal_row->>'saw_insert')::boolean
         OR NOT (journal_row->>'saw_update')::boolean
         OR (journal_row->>'saw_delete')::boolean
      THEN RAISE EXCEPTION 'persisted matching logical state requires one UPDATE'; END IF;
    END IF;
  END LOOP;
  IF contribution.requirement_state_only_changes<>derived_requirement_state_only
     OR contribution.group_state_only_changes<>derived_group_state_only
     OR contribution.claim_state_only_changes<>derived_claim_state_only
     OR contribution.groups_touched<>(SELECT count(DISTINCT entry.value->>'object_id')
          FROM jsonb_array_elements(logical_outputs) entry(value)
          WHERE entry.value->>'kind' IN (
            'group_state','group_certificate','group_binding'))
     OR contribution.claims_touched<>(SELECT count(DISTINCT entry.value->>'object_id')
          FROM jsonb_array_elements(logical_outputs) entry(value)
          WHERE entry.value->>'kind' IN (
            'claim_state','claim_certificate','claim_binding'))
     OR contribution.answers_touched<>(SELECT count(DISTINCT entry.value->>'object_id')
          FROM jsonb_array_elements(logical_outputs) entry(value)
          WHERE entry.value->>'kind'='answer_state')
     OR contribution.group_local_state_operations<>
          ((SELECT count(DISTINCT group_id) FROM unnest(ordinary_mask_groups) group_id)
           +(SELECT count(DISTINCT entry.value->>'object_id')
               FROM jsonb_array_elements(logical_outputs) entry(value)
              WHERE entry.value->>'kind'='group_binding'))
     OR contribution.group_certificate_only_changes<>(
          SELECT count(DISTINCT certificate.value->>'object_id')
          FROM jsonb_array_elements(logical_outputs) certificate(value)
          WHERE certificate.value->>'kind' IN ('group_certificate','group_binding')
            AND NOT EXISTS (SELECT 1 FROM jsonb_array_elements(logical_outputs) state(value)
              WHERE state.value->>'kind'='group_state'
                AND state.value->>'object_id'=certificate.value->>'object_id'))
     OR contribution.claim_certificate_only_changes<>(
          SELECT count(DISTINCT certificate.value->>'object_id')
          FROM jsonb_array_elements(logical_outputs) certificate(value)
          WHERE certificate.value->>'kind' IN ('claim_certificate','claim_binding')
            AND NOT EXISTS (SELECT 1 FROM jsonb_array_elements(logical_outputs) state(value)
              WHERE state.value->>'kind'='claim_state'
                AND state.value->>'object_id'=certificate.value->>'object_id'))
  THEN RAISE EXCEPTION 'persisted matching derived logical work counter mismatch'; END IF;
  SELECT count(*) INTO expected_count FROM jsonb_array_elements(logical_outputs) entry(value)
    WHERE entry.value->>'kind'='status_delta';
  SELECT count(*) INTO status_count FROM groundloop_status_delta
    WHERE epoch_id=selected_epoch AND revision=selected_result;
  IF status_count<>expected_count THEN
    RAISE EXCEPTION 'persisted matching status-delta cardinality mismatch'; END IF;
  FOR logical_output IN SELECT entry.value FROM jsonb_array_elements(logical_outputs) entry(value)
    WHERE entry.value->>'kind'='status_delta'
  LOOP
    logical_fields:=logical_output->'after'->'fields';
    IF NOT EXISTS (SELECT 1 FROM groundloop_status_delta delta
      WHERE delta.epoch_id=selected_epoch AND delta.revision=selected_result
        AND delta.event_id=logical_fields->0->'value'->>'value'
        AND delta.object_type=logical_fields->1->'value'->>'value'
        AND delta.object_id=logical_fields->2->'value'->>'value'
        AND delta.old_status=logical_fields->3->'value'->>'value'
        AND delta.new_status=logical_fields->4->'value'->>'value'
        AND delta.reason=logical_fields->5->'value'->>'value')
    THEN RAISE EXCEPTION 'persisted matching status-delta row mismatch'; END IF;
    logical_relation:=CASE logical_fields->1->'value'->>'value'
      WHEN 'claim' THEN 'groundloop_m5_working_claim_state'
      ELSE 'groundloop_m5_working_answer_state' END;
    key_value:=groundloop_m5_matching_journal_key(logical_relation,
      ARRAY[selected_epoch::text,logical_fields->2->'value'->>'value']);
    EXECUTE 'SELECT to_jsonb(j) FROM pg_temp.groundloop_m5_matching_change_journal j
              WHERE relation_name=$1 AND key_preimage=$2'
      INTO journal_row USING logical_relation,key_value;
    prior_status:=journal_row->'first_old'->>'status';
    IF prior_status IS NULL AND logical_fields->1->'value'->>'value'='claim' THEN
      SELECT state.status INTO prior_status FROM groundloop_m5_published_claim_state state
       WHERE state.claim_id=logical_fields->2->'value'->>'value'
         AND state.valid_from_epoch<=patch.before_epoch_id
         AND (state.valid_to_epoch IS NULL OR state.valid_to_epoch>patch.before_epoch_id);
    ELSIF prior_status IS NULL THEN
      SELECT state.status INTO prior_status FROM groundloop_m5_published_answer_state state
       WHERE state.answer_version_id=logical_fields->2->'value'->>'value'
         AND state.valid_from_epoch<=patch.before_epoch_id
         AND (state.valid_to_epoch IS NULL OR state.valid_to_epoch>patch.before_epoch_id);
    END IF;
    IF journal_row IS NULL
       OR journal_row->'final_new'->>'status'<>logical_fields->4->'value'->>'value'
       OR prior_status IS DISTINCT FROM logical_fields->3->'value'->>'value'
    THEN RAISE EXCEPTION 'persisted matching status delta is not derived from state OLD/NEW'; END IF;
  END LOOP;
  FOR logical_output IN SELECT entry.value
    FROM jsonb_array_elements(logical_outputs) entry(value)
    WHERE entry.value->>'kind' IN ('group_certificate','claim_certificate')
  LOOP
    logical_kind:=logical_output->>'kind';
    logical_fields:=logical_output->'after'->'fields';
    IF logical_output->'after'->>'tag'='none' THEN
      SELECT change.value->'children'->2->>'value' INTO prior_digest
        FROM jsonb_array_elements(
          logical_patch_parsed->'children'->0->'children') change(value)
       WHERE change.value->'children'->0->>'value'=logical_kind
         AND change.value->'children'->1->>'value'=logical_output->>'object_id';
      IF prior_digest IS NULL OR
         (logical_kind='group_certificate' AND
           (groundloop_m5_expected_group_certificate(prior_digest::char(64))<>prior_digest
            OR EXISTS (SELECT 1 FROM pg_temp.groundloop_m5_matching_change_journal
              WHERE relation_name IN ('groundloop_m5_group_certificate_artifact',
                'groundloop_m5_group_certificate_artifact_row')
                AND (final_new->>'certificate_digest'=prior_digest
                  OR first_old->>'certificate_digest'=prior_digest))))
         OR (logical_kind='claim_certificate' AND
           (groundloop_m5_expected_claim_certificate(prior_digest::char(64))<>prior_digest
            OR EXISTS (SELECT 1 FROM pg_temp.groundloop_m5_matching_change_journal
              WHERE relation_name='groundloop_m5_claim_certificate_artifact'
                AND (final_new->>'certificate_digest'=prior_digest
                  OR first_old->>'certificate_digest'=prior_digest))))
      THEN RAISE EXCEPTION 'persisted matching absent certificate output mismatch'; END IF;
      CONTINUE;
    END IF;
    logical_digest:=logical_fields->(CASE logical_kind WHEN 'group_certificate' THEN 4 ELSE 8 END)
      ->'value'->>'value';
    logical_relation:=CASE logical_kind WHEN 'group_certificate'
      THEN 'groundloop_m5_group_certificate_artifact'
      ELSE 'groundloop_m5_claim_certificate_artifact' END;
    key_value:=groundloop_m5_matching_journal_key(logical_relation,ARRAY[logical_digest]);
    EXECUTE 'INSERT INTO pg_temp.groundloop_m5_matching_expected_changes
      (relation_name,key_preimage) VALUES ($1,$2)' USING logical_relation,key_value;
    IF logical_kind='group_certificate' THEN
      IF groundloop_m5_expected_group_certificate(logical_digest::char(64))<>logical_digest
         OR NOT EXISTS (SELECT 1 FROM groundloop_m5_group_certificate_artifact a
           WHERE a.certificate_digest=logical_digest
             AND a.decision_policy_version=patch.decision_policy_version
             AND a.group_version_id=logical_output->>'object_id'
             AND a.requirement_count=jsonb_array_length(logical_fields->2->'value'->'children'))
      THEN RAISE EXCEPTION 'persisted matching group certificate artifact mismatch'; END IF;
    ELSE
      IF groundloop_m5_expected_claim_certificate(logical_digest::char(64))<>logical_digest
         OR NOT EXISTS (SELECT 1 FROM groundloop_m5_claim_certificate_artifact a
           WHERE a.certificate_digest=logical_digest
             AND a.decision_policy_version=patch.decision_policy_version
             AND a.claim_id=logical_output->>'object_id')
      THEN RAISE EXCEPTION 'persisted matching claim certificate artifact mismatch'; END IF;
    END IF;
    IF groundloop_m5_matching_certificate_preimage(
         logical_kind,logical_digest::char(64))<>
       groundloop_m5_matching_reencode_logical(logical_output->'after')
    THEN RAISE EXCEPTION 'persisted matching certificate byte mismatch'; END IF;
    EXECUTE 'SELECT count(*) FROM pg_temp.groundloop_m5_matching_change_journal
              WHERE relation_name=$1 AND key_preimage=$2'
      INTO journal_count USING logical_relation,key_value;
    IF journal_count NOT IN (0,1) THEN
      RAISE EXCEPTION 'persisted matching certificate artifact journal cardinality mismatch'; END IF;
    IF journal_count=1 AND EXISTS (
      SELECT 1 FROM pg_temp.groundloop_m5_matching_change_journal j
       WHERE j.relation_name=logical_relation AND j.key_preimage=key_value
         AND (j.first_operation<>'INSERT' OR j.last_operation<>'INSERT'
           OR j.mutation_count<>1 OR NOT j.saw_insert OR j.saw_update OR j.saw_delete))
    THEN RAISE EXCEPTION 'persisted matching certificate artifact requires one INSERT or reuse'; END IF;
    IF logical_kind='group_certificate' THEN
      FOR expected_counter IN 0..jsonb_array_length(logical_fields->2->'value'->'children')-1 LOOP
        EXECUTE 'INSERT INTO pg_temp.groundloop_m5_matching_expected_changes
          (relation_name,key_preimage) VALUES ($1,$2)'
          USING 'groundloop_m5_group_certificate_artifact_row',
            groundloop_m5_matching_journal_key(
              'groundloop_m5_group_certificate_artifact_row',
              ARRAY[logical_digest,expected_counter::text]);
      END LOOP;
      EXECUTE 'SELECT count(*) FROM pg_temp.groundloop_m5_matching_change_journal
                WHERE relation_name=''groundloop_m5_group_certificate_artifact_row''
                  AND final_new->>''certificate_digest''=$1'
        INTO expected_count USING logical_digest;
      IF expected_count<>(CASE WHEN journal_count=1 THEN
           jsonb_array_length(logical_fields->2->'value'->'children') ELSE 0 END)
      THEN RAISE EXCEPTION 'persisted matching inserted group certificate row journal mismatch'; END IF;
    END IF;
  END LOOP;
  FOREACH relation_name IN ARRAY ARRAY[
    'groundloop_m5_matching_image_working','groundloop_m5_matching_patch_artifact',
    'groundloop_m5_matching_work_contribution','groundloop_m5_matching_work_accumulator'
  ] LOOP
    EXECUTE 'SELECT count(*) FROM pg_temp.groundloop_m5_matching_change_journal WHERE relation_name=$1'
      INTO journal_count USING relation_name;
    IF journal_count<>1 THEN
      RAISE EXCEPTION 'persisted matching transition requires exactly one % row',relation_name;
    END IF;
  END LOOP;
  FOR relation_name,child IN
    SELECT 'groundloop_m5_matching_observation_working',unnest(patch.observation_change_preimages)
    UNION ALL SELECT 'groundloop_m5_matching_edge_working',unnest(patch.edge_change_preimages)
    UNION ALL SELECT 'groundloop_m5_matching_hash_mask_working',unnest(patch.mask_change_preimages)
    UNION ALL SELECT 'groundloop_m5_matching_hall_working',unnest(patch.hall_change_preimages)
  LOOP
    decoded_change:=groundloop_m5_matching_validate_change(child,
      CASE relation_name
        WHEN 'groundloop_m5_matching_observation_working' THEN 'observation'
        WHEN 'groundloop_m5_matching_edge_working' THEN 'edge'
        WHEN 'groundloop_m5_matching_hash_mask_working' THEN 'mask'
        ELSE 'hall' END);
    key_value:=groundloop_m5_matching_journal_key(relation_name,
      CASE relation_name
        WHEN 'groundloop_m5_matching_observation_working' THEN ARRAY[selected_epoch::text,decoded_change->>'outer_one']
        WHEN 'groundloop_m5_matching_edge_working' THEN ARRAY[selected_epoch::text,decoded_change->>'outer_one',decoded_change->>'outer_two']
        WHEN 'groundloop_m5_matching_hash_mask_working' THEN ARRAY[selected_epoch::text,decoded_change->>'outer_one',decoded_change->>'outer_two']
        ELSE ARRAY[selected_epoch::text,decoded_change->>'outer_one'] END);
    EXECUTE 'INSERT INTO pg_temp.groundloop_m5_matching_expected_changes
      (relation_name,key_preimage) VALUES ($1,$2)' USING relation_name,key_value;
    EXECUTE 'SELECT to_jsonb(j) FROM pg_temp.groundloop_m5_matching_change_journal j
              WHERE relation_name=$1 AND key_preimage=$2'
      INTO journal_row USING relation_name,key_value;
    IF journal_row IS NULL OR NOT groundloop_m5_matching_working_row_matches_point(
         relation_name,journal_row->'final_new',decoded_change,selected_epoch,selected_result)
       OR groundloop_m5_matching_working_point_preimage(
            relation_name,journal_row->'final_new')<>
          groundloop_m5_matching_encode_fields(
            groundloop_m5_matching_typed_fields(decoded_change->'after'->'node'))
    THEN RAISE EXCEPTION 'persisted matching physical journal/patch mismatch for %',relation_name; END IF;
    before_point_value:=decoded_change->'before';
    current_row_value:=NULL;
    IF relation_name='groundloop_m5_matching_observation_working' THEN
      SELECT to_jsonb(c) INTO current_row_value
        FROM groundloop_m5_matching_observation_current c
       WHERE c.observation_id=decoded_change->>'outer_one';
    ELSIF relation_name='groundloop_m5_matching_edge_working' THEN
      SELECT to_jsonb(c) INTO current_row_value
        FROM groundloop_m5_matching_edge_current c
       WHERE c.requirement_version_id=decoded_change->>'outer_one'
         AND c.text_hash=decoded_change->>'outer_two';
    ELSIF relation_name='groundloop_m5_matching_hash_mask_working' THEN
      SELECT to_jsonb(c) INTO current_row_value
        FROM groundloop_m5_matching_hash_mask_current c
       WHERE c.group_version_id=decoded_change->>'outer_one'
         AND c.text_hash=decoded_change->>'outer_two';
    ELSE
      SELECT to_jsonb(c) INTO current_row_value
        FROM groundloop_m5_matching_hall_current c
       WHERE c.group_version_id=decoded_change->>'outer_one';
    END IF;
    IF before_point_value='null'::jsonb THEN
      IF journal_row->>'first_operation'<>'INSERT'
         OR journal_row->>'last_operation'<>'INSERT'
         OR (journal_row->>'mutation_count')::integer<>1
         OR NOT (journal_row->>'saw_insert')::boolean
         OR (journal_row->>'saw_update')::boolean
         OR (journal_row->>'saw_delete')::boolean
      THEN RAISE EXCEPTION 'persisted matching absent before-image requires one INSERT'; END IF;
      IF journal_row->'first_old'<>'null'::jsonb OR current_row_value IS NOT NULL THEN
        RAISE EXCEPTION 'persisted matching absent before-image mismatch for %',relation_name; END IF;
    ELSIF before_point_value->>'layer'='working' THEN
      IF journal_row->>'first_operation'<>'UPDATE'
         OR journal_row->>'last_operation'<>'UPDATE'
         OR (journal_row->>'mutation_count')::integer<>1
         OR (journal_row->>'saw_insert')::boolean
         OR NOT (journal_row->>'saw_update')::boolean
         OR (journal_row->>'saw_delete')::boolean
      THEN RAISE EXCEPTION 'persisted matching working before-image requires one UPDATE'; END IF;
      IF NOT groundloop_m5_matching_working_row_matches_point(
        relation_name,journal_row->'first_old',decoded_change,
        (before_point_value->>'epoch')::bigint,
        (before_point_value->>'revision')::bigint,'before')
         OR groundloop_m5_matching_working_point_preimage(
              relation_name,journal_row->'first_old')<>
            groundloop_m5_matching_encode_fields(
              groundloop_m5_matching_typed_fields(before_point_value->'node'))
      THEN RAISE EXCEPTION 'persisted matching working before-image mismatch for %',relation_name; END IF;
    ELSE
      IF journal_row->>'first_operation'<>'INSERT'
         OR journal_row->>'last_operation'<>'INSERT'
         OR (journal_row->>'mutation_count')::integer<>1
         OR NOT (journal_row->>'saw_insert')::boolean
         OR (journal_row->>'saw_update')::boolean
         OR (journal_row->>'saw_delete')::boolean
      THEN RAISE EXCEPTION 'persisted matching current fallback requires one INSERT'; END IF;
      IF journal_row->'first_old'<>'null'::jsonb
         OR NOT groundloop_m5_matching_current_row_matches_point(
           relation_name,current_row_value,decoded_change)
         OR groundloop_m5_matching_current_point_preimage(
              relation_name,current_row_value)<>
            groundloop_m5_matching_encode_fields(
              groundloop_m5_matching_typed_fields(before_point_value->'node'))
      THEN RAISE EXCEPTION 'persisted matching current before-image mismatch for %',relation_name; END IF;
    END IF;
  END LOOP;
  FOREACH relation_name IN ARRAY ARRAY[
    'groundloop_m5_matching_observation_working','groundloop_m5_matching_edge_working',
    'groundloop_m5_matching_hash_mask_working','groundloop_m5_matching_hall_working'
  ] LOOP
    expected_count:=CASE relation_name
      WHEN 'groundloop_m5_matching_observation_working' THEN cardinality(patch.observation_change_preimages)
      WHEN 'groundloop_m5_matching_edge_working' THEN cardinality(patch.edge_change_preimages)
      WHEN 'groundloop_m5_matching_hash_mask_working' THEN cardinality(patch.mask_change_preimages)
      ELSE cardinality(patch.hall_change_preimages) END;
    EXECUTE 'SELECT count(*) FROM pg_temp.groundloop_m5_matching_change_journal WHERE relation_name=$1'
      INTO journal_count USING relation_name;
    IF journal_count<>expected_count THEN
      RAISE EXCEPTION 'persisted matching physical journal has missing or extra rows for %',relation_name; END IF;
  END LOOP;
  EXECUTE 'SELECT count(*) FROM (
      (SELECT relation_name,key_preimage
         FROM pg_temp.groundloop_m5_matching_expected_changes
        WHERE relation_name IN (
          ''groundloop_m5_matching_observation_working'',
          ''groundloop_m5_matching_edge_working'',
          ''groundloop_m5_matching_hash_mask_working'',
          ''groundloop_m5_matching_hall_working'')
       EXCEPT ALL
       SELECT relation_name,key_preimage
         FROM pg_temp.groundloop_m5_matching_change_journal
        WHERE relation_name IN (
          ''groundloop_m5_matching_observation_working'',
          ''groundloop_m5_matching_edge_working'',
          ''groundloop_m5_matching_hash_mask_working'',
          ''groundloop_m5_matching_hall_working''))
      UNION ALL
      (SELECT relation_name,key_preimage
         FROM pg_temp.groundloop_m5_matching_change_journal
        WHERE relation_name IN (
          ''groundloop_m5_matching_observation_working'',
          ''groundloop_m5_matching_edge_working'',
          ''groundloop_m5_matching_hash_mask_working'',
          ''groundloop_m5_matching_hall_working'')
       EXCEPT ALL
       SELECT relation_name,key_preimage
         FROM pg_temp.groundloop_m5_matching_expected_changes
        WHERE relation_name IN (
          ''groundloop_m5_matching_observation_working'',
          ''groundloop_m5_matching_edge_working'',
          ''groundloop_m5_matching_hash_mask_working'',
          ''groundloop_m5_matching_hall_working''))
    ) mismatch' INTO journal_count;
  IF journal_count<>0 THEN
    RAISE EXCEPTION 'persisted matching physical journal is not an exact set bijection'; END IF;
  EXECUTE 'SELECT count(*) FROM (
      (SELECT relation_name,key_preimage
         FROM pg_temp.groundloop_m5_matching_expected_changes
        WHERE relation_name IN (
          ''groundloop_m5_working_requirement_state'',
          ''groundloop_m5_working_group_state'',
          ''groundloop_m5_working_claim_state'',
          ''groundloop_m5_working_answer_state'',
          ''groundloop_m5_working_group_certificate_binding'',
          ''groundloop_m5_working_claim_certificate_binding'')
       EXCEPT ALL
       SELECT relation_name,key_preimage
         FROM pg_temp.groundloop_m5_matching_change_journal
        WHERE relation_name IN (
          ''groundloop_m5_working_requirement_state'',
          ''groundloop_m5_working_group_state'',
          ''groundloop_m5_working_claim_state'',
          ''groundloop_m5_working_answer_state'',
          ''groundloop_m5_working_group_certificate_binding'',
          ''groundloop_m5_working_claim_certificate_binding''))
      UNION ALL
      (SELECT relation_name,key_preimage
         FROM pg_temp.groundloop_m5_matching_change_journal
        WHERE relation_name IN (
          ''groundloop_m5_working_requirement_state'',
          ''groundloop_m5_working_group_state'',
          ''groundloop_m5_working_claim_state'',
          ''groundloop_m5_working_answer_state'',
          ''groundloop_m5_working_group_certificate_binding'',
          ''groundloop_m5_working_claim_certificate_binding'')
       EXCEPT ALL
       SELECT relation_name,key_preimage
         FROM pg_temp.groundloop_m5_matching_expected_changes
        WHERE relation_name IN (
          ''groundloop_m5_working_requirement_state'',
          ''groundloop_m5_working_group_state'',
          ''groundloop_m5_working_claim_state'',
          ''groundloop_m5_working_answer_state'',
          ''groundloop_m5_working_group_certificate_binding'',
          ''groundloop_m5_working_claim_certificate_binding''))
    ) mismatch' INTO journal_count;
  IF journal_count<>0 THEN
    RAISE EXCEPTION 'persisted matching logical journal is not an exact set bijection'; END IF;
  EXECUTE 'SELECT count(*) FROM (
      SELECT relation_name,key_preimage
        FROM pg_temp.groundloop_m5_matching_change_journal
       WHERE relation_name IN (
         ''groundloop_m5_group_certificate_artifact'',
         ''groundloop_m5_group_certificate_artifact_row'',
         ''groundloop_m5_claim_certificate_artifact'')
      EXCEPT ALL
      SELECT relation_name,key_preimage
        FROM pg_temp.groundloop_m5_matching_expected_changes
       WHERE relation_name IN (
         ''groundloop_m5_group_certificate_artifact'',
         ''groundloop_m5_group_certificate_artifact_row'',
         ''groundloop_m5_claim_certificate_artifact'')
    ) mismatch' INTO journal_count;
  IF journal_count<>0 THEN
    RAISE EXCEPTION 'persisted matching certificate journal has an extra row'; END IF;
  EXECUTE 'SELECT count(*) FROM (
      SELECT relation_name,key_preimage
        FROM pg_temp.groundloop_m5_matching_change_journal
      EXCEPT ALL
      SELECT relation_name,key_preimage
        FROM pg_temp.groundloop_m5_matching_expected_changes
    ) unconsumed' INTO journal_count;
  IF journal_count<>0 THEN
    RAISE EXCEPTION 'persisted matching transition has unconsumed journal rows'; END IF;
  EXECUTE 'UPDATE pg_temp.groundloop_m5_matching_transition_context
              SET validation_done=true
            WHERE backend_pid=$1 AND transaction_id=pg_current_xact_id()::text::bigint
              AND session_role=session_user'
    USING pg_backend_pid();
  RETURN NULL;
END;
$$;
REVOKE ALL ON FUNCTION groundloop_m5_matching_capture_transition_anchor()
  FROM PUBLIC;
REVOKE ALL ON FUNCTION groundloop_m5_matching_deferred_validate()
  FROM PUBLIC;
CREATE TRIGGER groundloop_m5_matching_runtime_anchor_capture
AFTER INSERT OR UPDATE ON groundloop_m5_runtime_epoch
FOR EACH ROW EXECUTE FUNCTION groundloop_m5_matching_capture_transition_anchor();
CREATE CONSTRAINT TRIGGER groundloop_m5_matching_runtime_anchor_validate
AFTER INSERT OR UPDATE ON groundloop_m5_runtime_epoch
DEFERRABLE INITIALLY DEFERRED FOR EACH ROW
EXECUTE FUNCTION groundloop_m5_matching_deferred_validate();
DO $$ DECLARE relation_name text; BEGIN
  FOREACH relation_name IN ARRAY ARRAY[
    'groundloop_m5_matching_image_working',
    'groundloop_m5_matching_observation_working',
    'groundloop_m5_matching_edge_working',
    'groundloop_m5_matching_hash_mask_working',
    'groundloop_m5_matching_hall_working',
    'groundloop_m5_matching_patch_artifact',
    'groundloop_m5_matching_work_contribution',
    'groundloop_m5_matching_work_accumulator',
    'groundloop_m5_working_requirement_state',
    'groundloop_m5_working_group_state',
    'groundloop_m5_working_claim_state',
    'groundloop_m5_working_answer_state',
    'groundloop_m5_group_certificate_artifact',
    'groundloop_m5_group_certificate_artifact_row',
    'groundloop_m5_claim_certificate_artifact',
    'groundloop_m5_working_group_certificate_binding',
    'groundloop_m5_working_claim_certificate_binding'
  ] LOOP
    EXECUTE format(
      'CREATE CONSTRAINT TRIGGER %I AFTER INSERT OR UPDATE OR DELETE ON %I '
      'DEFERRABLE INITIALLY DEFERRED FOR EACH ROW '
      'EXECUTE FUNCTION groundloop_m5_matching_deferred_validate()',
      relation_name||'_d25_transition_root',relation_name);
  END LOOP;
END $$;
