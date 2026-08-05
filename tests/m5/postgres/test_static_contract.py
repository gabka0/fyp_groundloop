"""Static physical-contract checks and live PostgreSQL index evidence."""

from __future__ import annotations

import ast
import json
import re
from pathlib import Path
from typing import Any

from psycopg import Connection

from groundloop.postgres.migrations import (
    LEGACY_MIGRATION_NAMES,
    M5_CATALOG_PREFLIGHT_ROW_EXCLUSIVE_RELATIONS,
    M5_INSTALL_LOCK_RELATIONS,
    M5_MIGRATION_LABEL,
    M5_ORACLE_LABEL,
)

ROOT = Path(__file__).resolve().parents[3]
MIGRATION = (ROOT / M5_MIGRATION_LABEL).read_text(encoding="utf-8")
ORACLE = (ROOT / M5_ORACLE_LABEL).read_text(encoding="utf-8")
INVENTORY_PATH = (
    ROOT / "docs/workstreams/m5_postgres_oracle/m4_shared_surface_inventory.json"
)

LOCK_ORDER = (
    "groundloop_epoch",
    "groundloop_m4_update",
    "groundloop_claim",
    "groundloop_semantic_observation",
    "groundloop_observation_currency",
    "groundloop_published_observation_currency",
    "groundloop_working_observation_delta",
)
CATALOG_ROW_EXCLUSIVE_LOCK_ORDER = (
    "groundloop_answer_version",
    "groundloop_m4_working_claim_state",
    "groundloop_m4_working_answer_state",
    "groundloop_m4_claim_admission_index",
    "groundloop_m4_verification_execution",
    "groundloop_m4_role_embedding_artifact",
    "groundloop_m4_document_metadata_overlay",
    "groundloop_m4_claim_registry_member",
    "groundloop_m4_structural_deactivation",
    "groundloop_m4_discovery_result",
    "groundloop_m4_claim_registry_snapshot",
    "groundloop_m4_event_audit_run",
    "groundloop_impact_evaluation_run",
    "groundloop_semantic_job",
    "groundloop_discovery_scope",
    "groundloop_m4_evaluation_epoch_counter",
    "groundloop_m4_evaluation_counter_transition",
)


def test_migration_has_exact_frozen_lock_order_before_open_epoch_guard() -> None:
    lock_pattern = re.compile(
        r"LOCK TABLE (groundloop_[a-z0-9_]+) IN ACCESS EXCLUSIVE MODE;"
    )
    locks = tuple(lock_pattern.findall(MIGRATION))
    assert locks == LOCK_ORDER
    assert M5_INSTALL_LOCK_RELATIONS == LOCK_ORDER
    final_lock = MIGRATION.index(
        "LOCK TABLE groundloop_working_observation_delta IN ACCESS EXCLUSIVE MODE;"
    )
    open_guard = MIGRATION.index("groundloop_m5_open_epoch_guard")
    assert final_lock < open_guard


def test_installer_keeps_writer_freeze_and_catalog_stabilization_lock_classes() -> None:
    assert M5_INSTALL_LOCK_RELATIONS == LOCK_ORDER
    assert (
        M5_CATALOG_PREFLIGHT_ROW_EXCLUSIVE_RELATIONS == CATALOG_ROW_EXCLUSIVE_LOCK_ORDER
    )
    assert not set(LOCK_ORDER) & set(CATALOG_ROW_EXCLUSIVE_LOCK_ORDER)
    source = (ROOT / "src/groundloop/postgres/migrations.py").read_text(
        encoding="utf-8"
    )
    exclusive_lock = source.index(
        'connection.execute(f"LOCK TABLE {relation} IN ACCESS EXCLUSIVE MODE")'
    )
    catalog_lock = source.index(
        'connection.execute(f"LOCK TABLE {relation} IN ROW EXCLUSIVE MODE")'
    )
    assert exclusive_lock < catalog_lock


def test_bundle_and_legacy_file_order_are_literal_and_m5_defines_no_enum() -> None:
    assert LEGACY_MIGRATION_NAMES == tuple(
        f"{number:03d}_{suffix}"
        for number, suffix in (
            (0, "extensions.sql"),
            (1, "m2_base.sql"),
            (2, "m3_static_ai.sql"),
            (3, "m4_dynamic_impact.sql"),
            (4, "m4_working_publication.sql"),
            (5, "m4_durable_execution.sql"),
            (6, "m4_role_embedding_artifacts.sql"),
            (7, "m4_structural_overlay.sql"),
            (8, "m4_atomic_discovery.sql"),
            (9, "m4_incremental_execution.sql"),
            (10, "m4_event_audit.sql"),
            (11, "m4_interval_exclusion.sql"),
            (12, "m4_point_runtime_counters.sql"),
            (13, "m4_evaluation_overlay.sql"),
        )
    )
    assert M5_MIGRATION_LABEL == "migrations/014_m5_evidence_groups.sql"
    assert M5_ORACLE_LABEL == "sql/m5/full_recompute_oracle.sql"
    assert not re.search(r"\b(?:CREATE|ALTER)\s+TYPE\b", MIGRATION, re.IGNORECASE)


def test_sql_oracle_uses_base_edges_and_distinct_recursive_assignment_state() -> None:
    assert "groundloop_m5_active_requirement_edge_oracle" in ORACLE
    assert "groundloop_m5_group_subset_hall_oracle" in ORACLE
    assignment_start = ORACLE.index(
        "CREATE OR REPLACE VIEW groundloop_m5_assignment_audit_oracle"
    )
    assignment_end = ORACLE.index(
        "CREATE OR REPLACE VIEW groundloop_m5_group_certificate_validity_oracle"
    )
    assignment_sql = ORACLE[assignment_start:assignment_end]
    assert "WITH RECURSIVE eligible AS" in assignment_sql
    assert "assignment_state (" in assignment_sql
    assert "UNION ALL" not in assignment_sql.upper()
    assert re.search(r"\bUNION\b", assignment_sql, re.IGNORECASE)
    assert "groundloop_m5_hall_mask" not in ORACLE
    assert "groundloop_m5_hall_materialized" not in ORACLE


def test_python_reference_matching_import_is_constructor_local_only() -> None:
    module_path = ROOT / "src/groundloop/postgres/m5.py"
    module = ast.parse(module_path.read_text(encoding="utf-8"))
    imports: list[tuple[str, str | None]] = []
    for node in ast.walk(module):
        if (
            isinstance(node, ast.ImportFrom)
            and node.module == "groundloop.m5.reference"
        ):
            owner = next(
                (
                    function.name
                    for function in ast.walk(module)
                    if isinstance(
                        function,
                        (ast.FunctionDef, ast.AsyncFunctionDef),
                    )
                    and node in tuple(ast.walk(function))
                ),
                None,
            )
            imports.append((node.names[0].name, owner))
    assert imports == [
        ("maximum_matching_with_unmatched_branch", "_build_bootstrap_certificates")
    ]
    assert "groundloop.m5.reference" not in ORACLE


def test_m5_public_activation_contract_is_not_redefined_by_postgres_lane() -> None:
    source = (ROOT / "src/groundloop/postgres/m5.py").read_text(encoding="utf-8")
    forbidden = (
        "M5ActivationRequest",
        "M5ActivationResult",
        "def activate_m5(",
        "m5-activation-v2",
        "m5-bootstrap-state-v1",
    )
    assert all(token not in source for token in forbidden)
    assert "M5BootstrapProjection" in source
    assert "bootstrap_state_hash" not in source


def test_out_of_scope_shared_table_inventory_matches_all_positional_inserts() -> None:
    inventory = json.loads(INVENTORY_PATH.read_text(encoding="utf-8"))
    tables = "|".join(re.escape(value) for value in inventory["shared_tables"])
    positional = re.compile(
        rf"INSERT\s+INTO\s+({tables})\b(?!\s*\()",
        re.IGNORECASE,
    )
    detected: set[tuple[str, int, str]] = set()
    for directory in ("src", "tests", "scripts"):
        for path in (ROOT / directory).rglob("*.py"):
            source = path.read_text(encoding="utf-8")
            for match in positional.finditer(source):
                detected.add(
                    (
                        path.relative_to(ROOT).as_posix(),
                        source.count("\n", 0, match.start()) + 1,
                        match.group(1).lower(),
                    )
                )

    recorded = {
        (entry["path"], int(entry["line"]), entry["table"])
        for section in (
            "blocking_runtime_findings",
            "nonblocking_source_audit_findings",
        )
        for entry in inventory[section]
        if entry["kind"] == "positional_insert"
    }
    recorded.update(
        (str(path), int(line), str(table))
        for path, line, table in inventory[
            "fixture_and_script_positional_insert_findings"
        ]
    )
    assert detected == recorded


def test_inventory_read_findings_remain_location_checkable_and_classified() -> None:
    inventory = json.loads(INVENTORY_PATH.read_text(encoding="utf-8"))
    findings = [
        entry
        for section in (
            "blocking_runtime_findings",
            "nonblocking_source_audit_findings",
        )
        for entry in inventory[section]
        if entry["kind"] != "positional_insert"
    ]
    ids = [str(entry["id"]) for entry in findings]
    assert len(ids) == len(set(ids))
    for entry in findings:
        line = (
            (ROOT / entry["path"])
            .read_text(encoding="utf-8")
            .splitlines()[int(entry["line"]) - 1]
        )
        assert entry["table"] in line


def test_resolved_runtime_inventory_has_location_checked_mechanical_evidence() -> None:
    inventory = json.loads(INVENTORY_PATH.read_text(encoding="utf-8"))
    assert inventory["status"] == "coordinator_patch_incorporated"
    assert inventory["blocking_runtime_findings"] == []
    resolved = inventory["resolved_runtime_findings"]
    assert len(resolved) == 10
    assert {entry["id"] for entry in resolved} == {
        *(f"M4-RUNTIME-READ-{number:03d}" for number in range(1, 9)),
        "M4-RUNTIME-WRITE-001",
        "M4-RUNTIME-WRITE-002",
    }
    for entry in resolved:
        lines = (ROOT / entry["path"]).read_text(encoding="utf-8").splitlines()
        line_index = int(entry["line"]) - 1
        assert entry["table"] in lines[line_index]
        assert entry["evidence"] in "\n".join(lines[line_index : line_index + 8])


def test_m5_indexes_are_valid_and_selected_when_sequential_scan_is_disabled(
    m5_connection: Connection[Any],
) -> None:
    expected_indexes = {
        "groundloop_m5_group_family_by_claim",
        "groundloop_m5_group_versions_by_family",
        "groundloop_m5_requirements_by_group",
        "groundloop_m5_requirements_by_text_hash",
        "groundloop_m5_group_certificate_by_group_policy",
        "groundloop_m5_group_certificate_row_observation",
        "groundloop_m5_claim_certificate_by_claim_policy",
        "groundloop_m5_current_requirement_observations",
    }
    rows = m5_connection.execute(
        """
        SELECT indexrelid::regclass::text, indisvalid, indisready
        FROM pg_index
        WHERE indexrelid::regclass::text = ANY(%s)
        ORDER BY indexrelid::regclass::text
        """,
        (sorted(expected_indexes),),
    ).fetchall()
    assert {str(row[0]) for row in rows} == expected_indexes
    assert all(bool(row[1]) and bool(row[2]) for row in rows)

    m5_connection.execute("SET LOCAL enable_seqscan = off")
    plan_rows = m5_connection.execute(
        """
        EXPLAIN (COSTS OFF)
        SELECT certificate_digest
        FROM groundloop_m5_group_certificate_artifact
        WHERE group_version_id = 'missing-group'
          AND decision_policy_version = 'missing-policy'
        """
    ).fetchall()
    plan = "\n".join(str(row[0]) for row in plan_rows)
    assert "groundloop_m5_group_certificate_by_group_policy" in plan
