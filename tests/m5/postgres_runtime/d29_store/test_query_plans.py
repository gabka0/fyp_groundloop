"""Live PostgreSQL plan evidence for D29's bounded locator surfaces."""

from __future__ import annotations

import hashlib
from typing import Any

from m5.postgres.helpers import insert_published_group, make_group

from groundloop.m5.runtime.contracts import (
    ActiveChunkSnapshot,
    ActiveChunkSnapshotEntry,
    M5TextNormalizerProvenance,
    RequirementRegistrySnapshot,
    RequirementRegistrySnapshotEntry,
)


def _plan(database: Any, statement: str, parameters: tuple[object, ...]) -> str:
    database.connection.execute("SET LOCAL enable_seqscan = off")
    rows = database.connection.execute(
        "EXPLAIN (COSTS OFF) " + statement, parameters
    ).fetchall()
    return "\n".join(str(row[0]) for row in rows)


def _assert_index_only(plan: str, index_name: str) -> None:
    assert index_name in plan
    assert "Seq Scan" not in plan


def _plan_nodes(plan: dict[str, object]) -> tuple[dict[str, object], ...]:
    nested = plan.get("Plans", [])
    assert isinstance(nested, list)
    children = tuple(
        child
        for item in nested
        if isinstance(item, dict)
        for child in _plan_nodes(item)
    )
    return (plan, *children)


def _analyzed_plan(
    database: Any,
    statement: str,
    parameters: tuple[object, ...],
    *,
    disable_seqscan: bool = True,
) -> tuple[dict[str, object], tuple[dict[str, object], ...]]:
    """Return the populated root and complete node tree for one bounded read."""

    if disable_seqscan:
        database.connection.execute("SET LOCAL enable_seqscan = off")
    explained = database.connection.execute(
        "EXPLAIN (ANALYZE, COSTS OFF, FORMAT JSON) " + statement,
        parameters,
    ).fetchone()
    assert explained is not None
    document = explained[0]
    assert isinstance(document, list) and len(document) == 1
    outer = document[0]
    assert isinstance(outer, dict) and isinstance(outer.get("Plan"), dict)
    root = outer["Plan"]
    assert isinstance(root, dict)
    return root, _plan_nodes(root)


def _assert_populated_index_range(
    root: dict[str, object],
    nodes: tuple[dict[str, object], ...],
    *,
    index_name: str,
    expected_rows: int,
) -> None:
    """Reject scans/aggregates and bind observed width to the selected range."""

    assert expected_rows > 0
    assert all(node.get("Node Type") not in {"Seq Scan", "Aggregate"} for node in nodes)
    index_nodes = tuple(node for node in nodes if node.get("Index Name") == index_name)
    assert len(index_nodes) == 1
    assert int(index_nodes[0]["Actual Rows"]) == expected_rows
    assert int(root["Actual Rows"]) == expected_rows


def test_admitted_pair_chunk_locator_uses_migration_018_index(
    d29_schema: Any,
) -> None:
    plan = _plan(
        d29_schema,
        '''SELECT chunk_version_id, admitted_pair_digest
             FROM groundloop_m5_requirement_admitted_pair
            WHERE chunk_version_id COLLATE "C" = %s
            ORDER BY admitted_pair_digest COLLATE "C"''',
        (str(d29_schema.first_m5["chunk_version_id"]),),
    )
    _assert_index_only(plan, "groundloop_m5_admitted_pair_by_chunk_edge")


def test_retained_direct_epoch_range_uses_migration_018_index(
    d29_schema: Any,
) -> None:
    plan = _plan(
        d29_schema,
        '''SELECT job_id
             FROM groundloop_semantic_job
            WHERE epoch_id = %s
            ORDER BY job_id COLLATE "C"''',
        (int(d29_schema.first_m4["epoch_id"]),),
    )
    _assert_index_only(plan, "groundloop_m4_job_by_epoch")


def test_retained_m5_epoch_state_range_uses_existing_index(
    d29_schema: Any,
) -> None:
    plan = _plan(
        d29_schema,
        '''WITH hydrated AS MATERIALIZED (
               SELECT logical_job_id, epoch_id, structural_event_id, job_kind,
                      candidate_policy_id, candidate_policy_manifest_hash,
                      parent_job_id, subject_kind::text, subject_id,
                      chunk_version_id, semantic_pair_digest,
                      admitted_pair_digest, scope_contract_digest,
                      requirement_registry_snapshot_digest,
                      active_chunk_snapshot_digest, role_template_hash,
                      execution_spec_hash, expandable, payload_hash, job_state,
                      result_artifact_id, result_artifact_hash,
                      scope_closure_digest, child_set_hash, archive_reason,
                      completion_digest, cancelled_by_event_id,
                      cancelled_by_epoch_id, cancellation_reason,
                      created_revision, completed_revision, created_at,
                      completed_at
                 FROM groundloop_m5_semantic_job
                WHERE epoch_id = %s
                ORDER BY job_state, logical_job_id
           )
           SELECT *
             FROM hydrated
            ORDER BY logical_job_id COLLATE "C"''',
        (int(d29_schema.first_m5["epoch_id"]),),
    )
    _assert_index_only(plan, "groundloop_m5_job_by_epoch_state")


def test_dependency_and_scope_points_use_named_primary_keys(
    d29_schema: Any,
) -> None:
    dependency_plan = _plan(
        d29_schema,
        '''SELECT epoch_id, parent_job_id, child_job_id
             FROM groundloop_semantic_job_dependency
            WHERE epoch_id = %s
            ORDER BY parent_job_id COLLATE "C", child_job_id COLLATE "C"''',
        (int(d29_schema.first_m4["epoch_id"]),),
    )
    scope_plan = _plan(
        d29_schema,
        "SELECT root_job_id FROM groundloop_discovery_scope WHERE root_job_id = %s",
        (str(d29_schema.first_m4["root_job_id"]),),
    )
    _assert_index_only(
        dependency_plan,
        "groundloop_semantic_job_dependency_pkey",
    )
    _assert_index_only(scope_plan, "groundloop_discovery_scope_pkey")


def test_first_application_dependencies_use_bounded_primary_key_routes(
    d29_schema: Any,
) -> None:
    connection = d29_schema.connection
    m4_epoch = int(d29_schema.first_m4["epoch_id"])
    m4_root = str(d29_schema.first_m4["root_job_id"])
    m5_epoch = int(d29_schema.first_m5["epoch_id"])
    m5_root = str(d29_schema.first_m5["forward_root_job_id"])
    m4_count_row = connection.execute(
        "SELECT count(*) FROM groundloop_semantic_job_dependency "
        "WHERE epoch_id = %s AND parent_job_id = %s",
        (m4_epoch, m4_root),
    ).fetchone()
    m5_count_row = connection.execute(
        "SELECT count(*) FROM groundloop_m5_job_dependency "
        "WHERE epoch_id = %s AND parent_job_id = %s",
        (m5_epoch, m5_root),
    ).fetchone()
    assert m4_count_row is not None and m5_count_row is not None
    m4_count = int(m4_count_row[0])
    m5_count = int(m5_count_row[0])
    assert m4_count > 0 and m5_count > 0
    m5_child_row = connection.execute(
        "SELECT child_job_id FROM groundloop_m5_job_dependency "
        "WHERE epoch_id = %s AND parent_job_id = %s "
        'ORDER BY child_job_id COLLATE "C" LIMIT 1',
        (m5_epoch, m5_root),
    ).fetchone()
    assert m5_child_row is not None
    m5_child = str(m5_child_row[0]).strip()

    m4_root_plan, m4_nodes = _analyzed_plan(
        d29_schema,
        '''SELECT epoch_id, parent_job_id, child_job_id
             FROM groundloop_semantic_job_dependency
            WHERE epoch_id = %s AND parent_job_id = %s
            ORDER BY child_job_id COLLATE "C"''',
        (m4_epoch, m4_root),
    )
    _assert_populated_index_range(
        m4_root_plan,
        m4_nodes,
        index_name="groundloop_semantic_job_dependency_pkey",
        expected_rows=m4_count,
    )
    m5_locator_root, m5_locator_nodes = _analyzed_plan(
        d29_schema,
        """SELECT epoch_id, parent_job_id, child_job_id
             FROM groundloop_m5_job_dependency
            WHERE ROW(epoch_id, parent_job_id, child_job_id)
                  > ROW(%s, %s, %s)
            ORDER BY epoch_id, parent_job_id, child_job_id
            LIMIT 1""",
        (m5_epoch, m5_root, ""),
    )
    _assert_populated_index_range(
        m5_locator_root,
        m5_locator_nodes,
        index_name="groundloop_m5_job_dependency_pkey",
        expected_rows=1,
    )
    m5_point_root, m5_point_nodes = _analyzed_plan(
        d29_schema,
        """WITH edge_point AS MATERIALIZED (
               SELECT epoch_id, parent_job_id, child_job_id
               FROM groundloop_m5_job_dependency
               WHERE ROW(epoch_id, parent_job_id, child_job_id)
                     >= ROW(%s, %s, %s)
               ORDER BY epoch_id, parent_job_id, child_job_id
               LIMIT 1
           )
           SELECT epoch_id, parent_job_id, child_job_id
           FROM edge_point
           WHERE epoch_id = %s AND parent_job_id = %s AND child_job_id = %s""",
        (m5_epoch, m5_root, m5_child, m5_epoch, m5_root, m5_child),
    )
    _assert_populated_index_range(
        m5_point_root,
        m5_point_nodes,
        index_name="groundloop_m5_job_dependency_pkey",
        expected_rows=1,
    )


def test_current_currency_and_frontier_locators_use_chunk_indexes(
    d29_schema: Any,
) -> None:
    connection = d29_schema.connection
    currency_chunk_row = connection.execute(
        "SELECT chunk_version_id FROM groundloop_observation_currency "
        'ORDER BY chunk_version_id COLLATE "C" LIMIT 1'
    ).fetchone()
    assert currency_chunk_row is not None
    chunk_id = str(currency_chunk_row[0])
    current_count_row = connection.execute(
        "SELECT count(*) FROM groundloop_observation_currency "
        "WHERE chunk_version_id = %s",
        (chunk_id,),
    ).fetchone()
    assert current_count_row is not None
    current_count = int(current_count_row[0])
    assert current_count > 0

    currency_root, currency_nodes = _analyzed_plan(
        d29_schema,
        '''SELECT subject_kind::text, subject_id, task_type, observation_id
             FROM groundloop_observation_currency
            WHERE chunk_version_id = %s
            ORDER BY subject_kind::text COLLATE "C",
                     subject_id COLLATE "C", task_type COLLATE "C",
                     observation_id COLLATE "C"''',
        (chunk_id,),
    )
    _assert_populated_index_range(
        currency_root,
        currency_nodes,
        index_name="groundloop_current_observations_by_chunk",
        expected_rows=current_count,
    )

    savepoint = "d29_frontier_plan_fixture"
    connection.execute(f"SAVEPOINT {savepoint}")
    try:
        source = connection.execute(
            """SELECT candidate_policy_id, claim_id, chunk_version_id
                 FROM groundloop_semantic_job
                WHERE job_kind = 'verify_pair'
                ORDER BY job_id COLLATE "C"
                LIMIT 1"""
        ).fetchone()
        assert source is not None
        creator = connection.execute(
            """INSERT INTO groundloop_epoch (
                   event_id, payload_hash, revision, structural_status,
                   semantic_status, evaluation_state, publication_mode, sealed_at
               ) VALUES (%s, %s, 0, 'committed', 'sealed', 'complete',
                         'strict', now())
               RETURNING epoch_id""",
            ("d29-frontier-plan-event", "f" * 64),
        ).fetchone()
        assert creator is not None
        creator_epoch = int(creator[0])
        registry = connection.execute(
            "SELECT claim_registry_snapshot_id FROM groundloop_candidate_policy "
            "WHERE candidate_policy_id = %s",
            (str(source[0]),),
        ).fetchone()
        assert registry is not None
        connection.execute(
            "ALTER TABLE groundloop_m4_update "
            "DISABLE TRIGGER groundloop_m4_update_runtime_mode_guard"
        )
        connection.execute(
            """INSERT INTO groundloop_m4_update (
                   epoch_id, update_kind, candidate_policy_id,
                   previous_published_epoch_id, registry_snapshot_id, manifest
               ) VALUES (%s, 'insert', %s, %s, %s, '{}'::jsonb)""",
            (
                creator_epoch,
                str(source[0]),
                d29_schema.database.base.epoch_id,
                str(registry[0]),
            ),
        )
        connection.execute(
            "ALTER TABLE groundloop_m4_update "
            "ENABLE TRIGGER groundloop_m4_update_runtime_mode_guard"
        )
        connection.execute(
            """INSERT INTO groundloop_candidate_frontier (
                   claim_id, chunk_version_id, candidate_policy_id,
                   frontier_state, rank, retrieval_score,
                   candidate_artifact_hash, valid_from_epoch, valid_to_epoch
               ) VALUES (%s, %s, %s, 'verified_current', 1, 1.0,
                         %s, %s, NULL)""",
            (str(source[1]), str(source[2]), str(source[0]), "e" * 64, creator_epoch),
        )
        frontier_root, frontier_nodes = _analyzed_plan(
            d29_schema,
            """SELECT claim_id, candidate_policy_id, valid_from_epoch
                 FROM groundloop_candidate_frontier
                WHERE chunk_version_id = %s AND valid_to_epoch IS NULL
                ORDER BY claim_id COLLATE "C", candidate_policy_id COLLATE "C",
                         valid_from_epoch""",
            (str(source[2]),),
        )
        assert all(node.get("Node Type") != "Seq Scan" for node in frontier_nodes)
        frontier_indexes = tuple(
            node
            for node in frontier_nodes
            if node.get("Index Name") == "groundloop_frontier_by_chunk_and_epoch"
        )
        assert len(frontier_indexes) == 1
        assert "chunk_version_id" in str(frontier_indexes[0].get("Index Cond"))
        assert int(frontier_root["Actual Rows"]) == 1
    finally:
        connection.execute(f"ROLLBACK TO SAVEPOINT {savepoint}")
        connection.execute(f"RELEASE SAVEPOINT {savepoint}")


def test_current_published_currency_uses_a_full_key_index_point(
    d29_schema: Any,
) -> None:
    key = d29_schema.connection.execute(
        """SELECT subject_kind::text, subject_id, chunk_version_id, task_type
             FROM groundloop_published_observation_currency
            WHERE valid_to_epoch IS NULL
            ORDER BY observation_id COLLATE "C"
            LIMIT 1"""
    ).fetchone()
    assert key is not None
    root, nodes = _analyzed_plan(
        d29_schema,
        """SELECT observation_id, valid_from_epoch, valid_to_epoch
             FROM groundloop_published_observation_currency
            WHERE subject_kind = %s
              AND subject_id = %s
              AND chunk_version_id = %s
              AND task_type = %s
              AND valid_to_epoch IS NULL""",
        tuple(key),
    )
    assert all(node.get("Node Type") != "Seq Scan" for node in nodes)
    point_indexes = tuple(
        node
        for node in nodes
        if node.get("Index Name")
        in {
            "groundloop_one_current_published_observation",
            "groundloop_published_observation_currency_no_overlap",
        }
    )
    assert len(point_indexes) == 1
    condition = str(point_indexes[0].get("Index Cond"))
    for column in ("subject_kind", "subject_id", "chunk_version_id", "task_type"):
        assert column in condition
    assert int(point_indexes[0]["Actual Rows"]) == int(root["Actual Rows"]) == 1


def test_m5_scope_and_provenance_points_use_named_indexes(
    d29_schema: Any,
) -> None:
    epoch_id = int(d29_schema.first_m5["epoch_id"])
    root_job_id = str(d29_schema.first_m5["forward_root_job_id"])
    scope_plan = _plan(
        d29_schema,
        "SELECT root_job_id FROM groundloop_m5_discovery_scope WHERE root_job_id = %s",
        (root_job_id,),
    )
    provenance_plan = _plan(
        d29_schema,
        "SELECT epoch_id, root_job_id "
        "FROM groundloop_m5_requirement_root_provenance "
        "WHERE epoch_id = %s AND root_job_id = %s",
        (epoch_id, root_job_id),
    )
    _assert_index_only(
        scope_plan,
        "groundloop_m5_discovery_scope_root_job_id_scope_contract_di_key",
    )
    _assert_index_only(
        provenance_plan,
        "groundloop_m5_requirement_root_provenance_pkey",
    )


def test_snapshot_and_publication_points_ignore_unrelated_history(
    d29_reservation_database: Any,
) -> None:
    database = d29_reservation_database
    connection = database.connection
    epoch_id = int(database.first_m5["epoch_id"])
    owner_row = connection.execute(
        "SELECT owner_claim_id "
        "FROM groundloop_m5_requirement_registry_snapshot_member "
        'ORDER BY owner_claim_id COLLATE "C" LIMIT 1'
    ).fetchone()
    assert owner_row is not None
    for group_ordinal in range(24):
        wide_group = make_group(
            group_id=f"d29-plan-wide-group-{group_ordinal}",
            family_id=f"d29-plan-wide-family-{group_ordinal}",
            claim_id=str(owner_row[0]),
            texts=tuple(
                f"D29 plan-only requirement {group_ordinal}-{ordinal}"
                for ordinal in range(8)
            ),
            requirement_ids=tuple(
                f"d29-plan-wide-requirement-{group_ordinal}-{ordinal}"
                for ordinal in range(8)
            ),
            source_id=f"d29-plan-wide-source-{group_ordinal}",
        )
        insert_published_group(connection, group=wide_group, epoch_id=epoch_id)
    requirement_rows = connection.execute(
        """SELECT requirement.requirement_version_id,
                  requirement.group_version_id, group_version.group_family_id,
                  family.claim_id, requirement.requirement_text
             FROM groundloop_m5_requirement_version AS requirement
             JOIN groundloop_m5_group_version AS group_version
               ON group_version.group_version_id = requirement.group_version_id
             JOIN groundloop_m5_group_family AS family
               ON family.group_family_id = group_version.group_family_id
            ORDER BY octet_length(requirement.requirement_version_id) DESC,
                     requirement.requirement_version_id COLLATE "C"
        """
    ).fetchall()
    unique_requirements: dict[str, tuple[object, ...]] = {}
    for row in requirement_rows:
        unique_requirements.setdefault(str(row[0]), tuple(row))
    assert len(unique_requirements) > 1
    requirement_snapshot = RequirementRegistrySnapshot.build(
        tuple(
            RequirementRegistrySnapshotEntry.build(
                requirement_version_id=str(row[0]),
                group_version_id=str(row[1]),
                group_family_id=str(row[2]),
                owner_claim_id=str(row[3]),
                requirement_text=str(row[4]),
            )
            for row in unique_requirements.values()
        )
    )
    connection.execute(
        """INSERT INTO groundloop_m5_requirement_registry_snapshot (
                   requirement_registry_snapshot_digest, requirement_count,
                   created_epoch_id
               ) VALUES (%s, %s, %s)
               ON CONFLICT DO NOTHING""",
        (
            requirement_snapshot.requirement_registry_snapshot_digest,
            requirement_snapshot.requirement_count,
            epoch_id,
        ),
    )
    for ordinal, entry in enumerate(requirement_snapshot.entries):
        connection.execute(
            """INSERT INTO groundloop_m5_requirement_registry_snapshot_member (
                       requirement_registry_snapshot_digest, member_ordinal,
                       requirement_version_id, group_version_id, group_family_id,
                       owner_claim_id, normalized_requirement_text,
                       requirement_text_hash
                   ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                   ON CONFLICT DO NOTHING""",
            (
                requirement_snapshot.requirement_registry_snapshot_digest,
                ordinal,
                entry.requirement_version_id,
                entry.group_version_id,
                entry.group_family_id,
                entry.owner_claim_id,
                entry.normalized_requirement_text,
                entry.requirement_text_hash,
            ),
        )

    connection.execute(
        """INSERT INTO groundloop_document (
                   document_id, source_uri, authority_class
               ) VALUES (%s, %s, %s)""",
        ("d29-plan-wide-document", "fixture://d29-plan", "test"),
    )
    connection.execute(
        """INSERT INTO groundloop_document_version (
                   document_version_id, document_id, content_hash,
                   valid_from_epoch, valid_to_epoch
               ) VALUES (%s, %s, %s, %s, NULL)""",
        (
            "d29-plan-wide-document-version",
            "d29-plan-wide-document",
            hashlib.sha256(b"d29-plan-wide-document").hexdigest(),
            epoch_id,
        ),
    )
    for chunk_ordinal in range(192):
        chunk_text = f"D29 plan-only chunk {chunk_ordinal}"
        connection.execute(
            """INSERT INTO groundloop_chunk_version (
                       chunk_version_id, document_version_id, chunk_index,
                       text, text_hash, chunker_version, valid_from_epoch,
                       valid_to_epoch
                   ) VALUES (%s, %s, %s, %s, %s, %s, %s, NULL)""",
            (
                f"d29-plan-wide-chunk-{chunk_ordinal}",
                "d29-plan-wide-document-version",
                chunk_ordinal,
                chunk_text,
                hashlib.sha256(chunk_text.encode()).hexdigest(),
                "d29-plan-test-chunker-v1",
                epoch_id,
            ),
        )
    chunk_rows = connection.execute(
        """SELECT chunk.chunk_version_id, chunk.text
             FROM groundloop_chunk_version AS chunk
            WHERE chunk.valid_to_epoch IS NULL
            ORDER BY octet_length(chunk.chunk_version_id) DESC,
                     chunk.chunk_version_id COLLATE "C"
        """
    ).fetchall()
    unique_chunks: dict[str, tuple[object, ...]] = {}
    for row in chunk_rows:
        unique_chunks.setdefault(str(row[0]), tuple(row))
    assert len(unique_chunks) > 1
    chunk_snapshot = ActiveChunkSnapshot.build(
        tuple(
            ActiveChunkSnapshotEntry.build(
                chunk_version_id=str(row[0]), chunk_text=str(row[1])
            )
            for row in unique_chunks.values()
        )
    )
    normalizer = M5TextNormalizerProvenance()
    connection.execute(
        """INSERT INTO groundloop_m5_active_chunk_snapshot (
                   active_chunk_snapshot_digest, chunk_count, created_epoch_id,
                   normalizer_id, normalizer_provenance_hash
               ) VALUES (%s, %s, %s, %s, %s)
               ON CONFLICT DO NOTHING""",
        (
            chunk_snapshot.active_chunk_snapshot_digest,
            chunk_snapshot.chunk_count,
            epoch_id,
            normalizer.normalizer_id,
            normalizer.normalizer_provenance_hash,
        ),
    )
    for ordinal, entry in enumerate(chunk_snapshot.entries):
        connection.execute(
            """INSERT INTO groundloop_m5_active_chunk_snapshot_member (
                       active_chunk_snapshot_digest, member_ordinal,
                       chunk_version_id, text_hash
                   ) VALUES (%s, %s, %s, %s)
                   ON CONFLICT DO NOTHING""",
            (
                chunk_snapshot.active_chunk_snapshot_digest,
                ordinal,
                entry.chunk_version_id,
                entry.text_hash,
            ),
        )

    requirement_digest = requirement_snapshot.requirement_registry_snapshot_digest
    chunk_digest = chunk_snapshot.active_chunk_snapshot_digest
    requirement_member = connection.execute(
        "SELECT requirement_version_id "
        "FROM groundloop_m5_requirement_registry_snapshot_member "
        "WHERE requirement_registry_snapshot_digest = %s "
        "ORDER BY octet_length(requirement_version_id) DESC, "
        'requirement_version_id COLLATE "C" LIMIT 1',
        (requirement_digest,),
    ).fetchone()
    chunk_member = connection.execute(
        "SELECT chunk_version_id "
        "FROM groundloop_m5_active_chunk_snapshot_member "
        "WHERE active_chunk_snapshot_digest = %s "
        "ORDER BY octet_length(chunk_version_id) DESC, "
        'chunk_version_id COLLATE "C" LIMIT 1',
        (chunk_digest,),
    ).fetchone()
    assert requirement_member is not None
    assert chunk_member is not None
    requirement_id = str(requirement_member[0])
    chunk_id = str(chunk_member[0])
    assert len(requirement_id) >= 256
    assert len(chunk_id) >= 1_900

    requirement_snapshot_count = connection.execute(
        "SELECT count(*) FROM groundloop_m5_requirement_registry_snapshot"
    ).fetchone()
    requirement_member_count = connection.execute(
        "SELECT count(*) FROM groundloop_m5_requirement_registry_snapshot_member"
    ).fetchone()
    selected_requirement_member_count = connection.execute(
        "SELECT count(*) "
        "FROM groundloop_m5_requirement_registry_snapshot_member "
        "WHERE requirement_registry_snapshot_digest = %s",
        (requirement_digest,),
    ).fetchone()
    selected_chunk_member_count = connection.execute(
        "SELECT count(*) "
        "FROM groundloop_m5_active_chunk_snapshot_member "
        "WHERE active_chunk_snapshot_digest = %s",
        (chunk_digest,),
    ).fetchone()
    epoch_count = connection.execute("SELECT count(*) FROM groundloop_epoch").fetchone()
    assert requirement_snapshot_count is not None
    assert requirement_member_count is not None
    assert selected_requirement_member_count is not None
    assert selected_chunk_member_count is not None
    assert epoch_count is not None
    assert int(requirement_snapshot_count[0]) > 1
    assert int(requirement_member_count[0]) > 1
    selected_requirement_width = int(selected_requirement_member_count[0])
    selected_chunk_width = int(selected_chunk_member_count[0])
    assert selected_requirement_width > 1
    assert selected_chunk_width > 1
    assert int(epoch_count[0]) > 1

    connection.execute("SET LOCAL enable_seqscan = on")
    connection.execute("SET LOCAL enable_indexscan = on")
    connection.execute("SET LOCAL enable_bitmapscan = on")
    connection.execute("ANALYZE groundloop_m5_requirement_registry_snapshot_member")
    connection.execute("ANALYZE groundloop_m5_active_chunk_snapshot_member")
    planner_state = connection.execute(
        "SELECT current_setting('enable_seqscan'), "
        "current_setting('enable_indexscan'), "
        "current_setting('enable_bitmapscan')"
    ).fetchone()
    assert planner_state == ("on", "on", "on")

    requirement_root, requirement_nodes = _analyzed_plan(
        database,
        """WITH member_point AS MATERIALIZED (
               SELECT requirement_registry_snapshot_digest,
                      requirement_version_id
               FROM groundloop_m5_requirement_registry_snapshot_member
               WHERE ROW(requirement_registry_snapshot_digest,
                         requirement_version_id) >= ROW(%s, %s)
               ORDER BY requirement_registry_snapshot_digest,
                        requirement_version_id
               LIMIT 1
           )
           SELECT requirement_version_id
           FROM member_point
           WHERE requirement_registry_snapshot_digest = %s
             AND requirement_version_id = %s""",
        (requirement_digest, requirement_id, requirement_digest, requirement_id),
        disable_seqscan=False,
    )
    assert all(
        node.get("Node Type")
        not in {
            "Seq Scan",
            "Bitmap Heap Scan",
            "Bitmap Index Scan",
            "Sort",
            "Aggregate",
        }
        for node in requirement_nodes
    ), tuple(
        {
            "node": node.get("Node Type"),
            "index": node.get("Index Name"),
            "rows": node.get("Actual Rows"),
            "removed": node.get("Rows Removed by Filter"),
            "condition": node.get("Index Cond"),
        }
        for node in requirement_nodes
    )
    requirement_point_nodes = tuple(
        node
        for node in requirement_nodes
        if node.get("Index Name")
        == "groundloop_m5_requirement_registry_snapshot_member_pkey"
    )
    assert len(requirement_point_nodes) == 1
    requirement_point = requirement_point_nodes[0]
    requirement_condition = str(requirement_point.get("Index Cond"))
    assert "requirement_registry_snapshot_digest" in requirement_condition
    assert "requirement_version_id" in requirement_condition
    assert int(requirement_point["Actual Rows"]) == 1
    assert int(requirement_point.get("Rows Removed by Filter", 0)) == 0
    assert int(requirement_root["Actual Rows"]) == 1
    assert all(
        node.get("Index Name")
        != "groundloop_m5_requirement_reg_requirement_registry_snapshot_key"
        for node in requirement_nodes
    )

    chunk_root, chunk_nodes = _analyzed_plan(
        database,
        """WITH member_point AS MATERIALIZED (
               SELECT active_chunk_snapshot_digest, chunk_version_id
               FROM groundloop_m5_active_chunk_snapshot_member
               WHERE ROW(active_chunk_snapshot_digest, chunk_version_id)
                     >= ROW(%s, %s)
               ORDER BY active_chunk_snapshot_digest, chunk_version_id
               LIMIT 1
           )
           SELECT chunk_version_id
           FROM member_point
           WHERE active_chunk_snapshot_digest = %s
             AND chunk_version_id = %s""",
        (chunk_digest, chunk_id, chunk_digest, chunk_id),
        disable_seqscan=False,
    )
    assert all(
        node.get("Node Type")
        not in {
            "Seq Scan",
            "Bitmap Heap Scan",
            "Bitmap Index Scan",
            "Sort",
            "Aggregate",
        }
        for node in chunk_nodes
    ), tuple(
        {
            "node": node.get("Node Type"),
            "index": node.get("Index Name"),
            "rows": node.get("Actual Rows"),
            "removed": node.get("Rows Removed by Filter"),
            "condition": node.get("Index Cond"),
        }
        for node in chunk_nodes
    )
    chunk_point_nodes = tuple(
        node
        for node in chunk_nodes
        if node.get("Index Name") == "groundloop_m5_active_chunk_snapshot_member_pkey"
    )
    assert len(chunk_point_nodes) == 1
    chunk_point = chunk_point_nodes[0]
    chunk_condition = str(chunk_point.get("Index Cond"))
    assert "active_chunk_snapshot_digest" in chunk_condition
    assert "chunk_version_id" in chunk_condition
    assert int(chunk_point["Actual Rows"]) == 1
    assert int(chunk_point.get("Rows Removed by Filter", 0)) == 0
    assert int(chunk_root["Actual Rows"]) == 1
    assert all(
        node.get("Index Name")
        != "groundloop_m5_active_chunk_sn_active_chunk_snapshot_digest__key"
        for node in chunk_nodes
    )

    point_reads = (
        (
            "SELECT requirement_registry_snapshot_digest "
            "FROM groundloop_m5_requirement_registry_snapshot "
            "WHERE requirement_registry_snapshot_digest = %s",
            (requirement_digest,),
            "groundloop_m5_requirement_registry_snapshot_pkey",
        ),
        (
            "SELECT active_chunk_snapshot_digest "
            "FROM groundloop_m5_active_chunk_snapshot "
            "WHERE active_chunk_snapshot_digest = %s",
            (chunk_digest,),
            "groundloop_m5_active_chunk_snapshot_pkey",
        ),
        (
            "SELECT epoch_id FROM groundloop_epoch WHERE epoch_id = %s",
            (epoch_id,),
            "groundloop_epoch_pkey",
        ),
        (
            "SELECT epoch_id FROM groundloop_m4_publication_head WHERE singleton",
            (),
            "groundloop_m4_publication_head_pkey",
        ),
        (
            "SELECT epoch_id FROM groundloop_m5_publication_head WHERE singleton",
            (),
            "groundloop_m5_publication_head_pkey",
        ),
    )
    for statement, parameters, index_name in point_reads:
        root, nodes = _analyzed_plan(database, statement, parameters)
        _assert_populated_index_range(
            root,
            nodes,
            index_name=index_name,
            expected_rows=1,
        )


def test_populated_admitted_history_keeps_chunk_locator_bounded(
    d29_schema: Any,
) -> None:
    connection = d29_schema.connection
    hot_chunk = str(d29_schema.first_m5["chunk_version_id"])
    hot_count_row = connection.execute(
        "SELECT count(*) FROM groundloop_m5_requirement_admitted_pair "
        "WHERE chunk_version_id = %s",
        (hot_chunk,),
    ).fetchone()
    total_count_row = connection.execute(
        "SELECT count(*) FROM groundloop_m5_requirement_admitted_pair"
    ).fetchone()
    assert hot_count_row is not None and total_count_row is not None
    hot_count = int(hot_count_row[0])
    assert int(total_count_row[0]) > hot_count > 0

    root, nodes = _analyzed_plan(
        d29_schema,
        '''SELECT chunk_version_id, admitted_pair_digest
             FROM groundloop_m5_requirement_admitted_pair
            WHERE chunk_version_id COLLATE "C" = %s
            ORDER BY admitted_pair_digest COLLATE "C"''',
        (hot_chunk,),
    )
    _assert_populated_index_range(
        root,
        nodes,
        index_name="groundloop_m5_admitted_pair_by_chunk_edge",
        expected_rows=hot_count,
    )


def test_unrelated_m5_root_and_child_history_keeps_epoch_range_bounded(
    d29_reservation_database: Any,
) -> None:
    database = d29_reservation_database
    connection = database.connection
    hot_epoch = int(database.first_m5["epoch_id"])
    unrelated_epoch = int(database.excluded_m5["epoch_id"])
    assert hot_epoch != unrelated_epoch
    hot_shape = connection.execute(
        """
        SELECT count(*),
               count(*) FILTER (WHERE parent_job_id IS NULL),
               count(*) FILTER (WHERE parent_job_id IS NOT NULL)
        FROM groundloop_m5_semantic_job
        WHERE epoch_id = %s
        """,
        (hot_epoch,),
    ).fetchone()
    unrelated_count = connection.execute(
        "SELECT count(*) FROM groundloop_m5_semantic_job WHERE epoch_id = %s",
        (unrelated_epoch,),
    ).fetchone()
    assert hot_shape is not None and unrelated_count is not None
    hot_count = int(hot_shape[0])
    assert hot_count > 0
    assert int(hot_shape[1]) > 0
    assert int(hot_shape[2]) > 0
    assert int(unrelated_count[0]) > 0

    root, nodes = _analyzed_plan(
        database,
        """SELECT logical_job_id, job_state
             FROM groundloop_m5_semantic_job
            WHERE epoch_id = %s
            ORDER BY job_state, logical_job_id""",
        (hot_epoch,),
    )
    _assert_populated_index_range(
        root,
        nodes,
        index_name="groundloop_m5_job_by_epoch_state",
        expected_rows=hot_count,
    )


def test_large_unrelated_m4_job_history_keeps_hot_epoch_range_bounded(
    d29_schema: Any,
) -> None:
    connection = d29_schema.connection
    hot_epoch = int(d29_schema.first_m4["epoch_id"])
    hot_count_row = connection.execute(
        "SELECT count(*) FROM groundloop_semantic_job WHERE epoch_id = %s",
        (hot_epoch,),
    ).fetchone()
    assert hot_count_row is not None
    hot_count = int(hot_count_row[0])
    assert hot_count > 0
    savepoint = "d29_large_unrelated_m4_history"
    connection.execute(f"SAVEPOINT {savepoint}")
    try:
        unrelated_epoch = int(d29_schema.first_m5["epoch_id"])
        assert unrelated_epoch != hot_epoch
        inserted = connection.execute(
            """
            INSERT INTO groundloop_semantic_job (
                job_id, epoch_id, parent_job_id, job_kind,
                candidate_policy_id, payload_hash, execution_spec_hash,
                claim_id, chunk_version_id, expandable, job_state,
                child_closed, created_revision
            )
            SELECT 'd29-unrelated-job-' || series.value::text,
                   %s, NULL, 'frontier_retrieve', source.candidate_policy_id,
                   source.payload_hash, source.execution_spec_hash,
                   source.claim_id, NULL, true, 'declared', false, 0
            FROM generate_series(1, 512) AS series(value)
            CROSS JOIN LATERAL (
                SELECT candidate_policy_id, payload_hash,
                       execution_spec_hash, claim_id
                FROM groundloop_semantic_job
                WHERE epoch_id = %s AND job_kind = 'frontier_retrieve'
                ORDER BY job_id COLLATE "C"
                LIMIT 1
            ) AS source
            """,
            (unrelated_epoch, hot_epoch),
        ).rowcount
        assert inserted == 512
        total_row = connection.execute(
            "SELECT count(*) FROM groundloop_semantic_job"
        ).fetchone()
        assert total_row is not None and int(total_row[0]) >= 512 + hot_count

        connection.execute("SET LOCAL enable_seqscan = off")
        explained = connection.execute(
            """
            EXPLAIN (ANALYZE, COSTS OFF, FORMAT JSON)
            SELECT job_id
            FROM groundloop_semantic_job
            WHERE epoch_id = %s
            ORDER BY job_id COLLATE "C"
            """,
            (hot_epoch,),
        ).fetchone()
        assert explained is not None
        document = explained[0]
        assert isinstance(document, list) and len(document) == 1
        root = document[0]
        assert isinstance(root, dict) and isinstance(root.get("Plan"), dict)
        nodes = _plan_nodes(root["Plan"])
        assert all(node.get("Node Type") != "Seq Scan" for node in nodes)
        index_nodes = tuple(
            node
            for node in nodes
            if node.get("Index Name") == "groundloop_m4_job_by_epoch"
        )
        assert len(index_nodes) == 1
        assert int(index_nodes[0]["Actual Rows"]) == hot_count
        assert int(root["Plan"]["Actual Rows"]) == hot_count
    finally:
        connection.execute(f"ROLLBACK TO SAVEPOINT {savepoint}")
        connection.execute(f"RELEASE SAVEPOINT {savepoint}")
