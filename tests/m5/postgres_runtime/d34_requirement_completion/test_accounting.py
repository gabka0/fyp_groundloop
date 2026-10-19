"""Actual ordered accounting and every-DML rollback qualification."""

import inspect
import re
from collections import Counter
from contextlib import contextmanager
from typing import Any

import pytest

from groundloop.m5.runtime.contracts import M5RuntimeTiming, M5RuntimeWork
from tests.m5.postgres_runtime.d24_application.conftest import database_snapshot
from tests.m5.postgres_runtime.d34_requirement_completion.conftest import capture_sql


@contextmanager
def native_producer_observer(monkeypatch: pytest.MonkeyPatch) -> Any:
    """Count actual first producers, independently of D34's sizing ledger.

    Frozen recipe arguments and actual returned preimages are the observations.
    Constructor checks, retained hydration and write-plan validation are not
    new producers. No D34 region builder is consulted for the expected total.
    """
    from groundloop.m5 import digests as base
    from groundloop.m5 import domain
    from groundloop.m5.runtime import digests
    from groundloop.m5.runtime import postgres_matching as matching

    sizes: Counter[str] = Counter()
    calls: Counter[str] = Counter()
    trace: list[Any] = []

    def stack() -> tuple[list[Any], list[str]]:
        frames = inspect.stack(context=0)[2:]
        return frames, [f.function for f in frames]

    def producer(domain_tag: str, frames: list[Any], names: list[str]) -> bool:
        constructors = [
            f.frame.f_locals.get("self")
            for f in frames
            if f.function == "__post_init__"
        ]
        if "_d34_complete_work" in names:
            return domain_tag == "m5-runtime-work-v2" and any(
                type(v) is M5RuntimeWork and v.work_digest == "" for v in constructors
            )
        if "_validate_prepared_matching_write_plan" in names:
            return False
        if "validate_timing" in names or "_d34_validate_source_observation" in names:
            return False
        if any(
            f.function == "_lock_requirement_intent_rows"
            and "preliminary_transition" in f.frame.f_locals
            for f in frames
        ):
            # The second logical reconstruction checks the already-produced
            # preliminary certificate after the native ordered answer lock.
            return False
        if constructors:
            return (
                domain_tag == "m5-group-certificate-v1"
                and any(
                    type(v) is domain.GroupMatchingCertificateArtifact
                    and v.certificate_digest == ""
                    for v in constructors
                )
                and (
                    "_requirement_intent_preview" in names
                    or "_requirement_preview_from_locked_intent" in names
                    or "_requirement_first_application_plan" in names
                )
            ) or (
                domain_tag == "m5-claim-certificate-v2"
                and any(
                    type(v) is domain.ClaimCertificateArtifact
                    and v.certificate_digest == ""
                    for v in constructors
                )
                and "_claim_artifact_for_requirement" in names
            )
        if domain_tag in {
            "m5-attempt-result-artifact-v2",
            "m5-attempt-result-id-v2",
            "m5-job-completion-v2",
        }:
            return "build" in names and "_d34_apply_current_completion" in names
        if domain_tag == "m5-requirement-semantic-observation-v2":
            return "_persist_verifier_closure" in names
        if domain_tag in {
            "m5-runtime-timing-observation-v1",
            "m5-attempt-runtime-timing-v1",
            "m5-attempt-execution-evidence-v1",
        }:
            return "build_requirement_execution_accounting" in names or (
                "_d34_finish_ordered_accounting" in names
                and "_resolve_pending_anchor" in names
            )
        if domain_tag == "m5-transition-call-timing-v1":
            return "_resolve_pending_anchor" in names
        if domain_tag == "m5-runtime-work-contribution-key-v1":
            return (
                "build" in names and "build_requirement_execution_accounting" in names
            ) or any(
                n in names
                for n in (
                    "_d34_insert_execution_contribution",
                    "_insert_verifier_completion_contribution",
                )
            )
        if domain_tag == "m5-persisted-matching-transition-intent-v1":
            return "_derive_matching_transition_intent_core" in names
        if domain_tag == "m5-matching-work-contribution-v1":
            return "_matching_contribution" in names
        if domain_tag == "m5-matching-work-v1":
            if any(
                f.function == "_insert_or_advance_matching_accumulator"
                and "accumulated" not in f.frame.f_locals
                for f in frames
            ):
                return False  # Retained prior vector validation, not the sum.
            return any(
                n in names
                for n in (
                    "_build_patch_artifact",
                    "_insert_contribution",
                    "_insert_or_advance_matching_accumulator",
                )
            )
        return "_requirement_first_application_plan" in names and domain_tag.startswith(
            (
                "m5-persisted-matching-",
                "m5-persisted-logical-overlay-",
                "m5-requirement-state-",
                "m5-group-state-",
                "m5-claim-state-",
                "m5-answer-state-",
                "m5-persisted-certificate-binding-",
            )
        )

    original_digest = base.stable_m5_digest

    def digest_spy(tag: str, *fields: Any) -> str:
        frames, names = stack()
        counted = producer(tag, frames, names)
        trace.append((tag, counted, tuple(names[:8])))
        if counted:
            size = len(digests.stable_m5_preimage(tag, *fields))
            sizes["hashed"] += size
            sizes["serialized"] += size
            calls[tag] += 1
        return original_digest(tag, *fields)

    with monkeypatch.context() as patch:
        for module in (base, digests, domain):
            patch.setattr(module, "stable_m5_digest", digest_spy)
        for name in (
            "matching_change_preimage",
            "matching_group_shape_set_preimage",
            "logical_overlay_patch_preimage",
            "logical_output_preimage",
            "persisted_matching_patch_preimage",
        ):
            original = getattr(digests, name)

            def preimage_spy(
                *args: Any, _original: Any = original, _name: str = name, **kw: Any
            ) -> bytes:
                result = _original(*args, **kw)
                frames, names = stack()
                if (
                    "_d34_complete_work" not in names
                    and "_validate_prepared_matching_write_plan" not in names
                    and "__post_init__" not in names
                    and "_requirement_first_application_plan" in names
                ):
                    sizes["serialized"] += len(result)
                    calls[_name] += 1
                    if (
                        _name == "logical_output_preimage"
                        and frames[0].function == "_build_patch_artifact"
                    ):
                        sizes["hashed"] += len(result)
                return result

            patch.setattr(digests, name, preimage_spy)
        original_absence = matching._matching_absence_lock_key

        def absence_spy(relation: str, key: Any) -> int:
            result = original_absence(relation, key)
            if (
                inspect.currentframe().f_back.f_code.co_name
                == "_reserve_matching_absence"
            ):
                size = len(b"m5-d28-matching-absence-v1") + sum(
                    8 + len(str(v).encode()) for v in (relation, *key)
                )
                sizes["hashed"] += size
                sizes["serialized"] += size
                calls["native_absence"] += 1
            return result

        patch.setattr(matching, "_matching_absence_lock_key", absence_spy)
        original_sha = matching.hashlib.sha256

        def sha_spy(data: bytes = b"", *args: Any, **kw: Any) -> Any:
            if (
                inspect.currentframe().f_back.f_code.co_name
                == "_d34_archive_coordinates"
            ):
                _, names = stack()
                if "_persist_verifier_closure" in names:
                    sizes["hashed"] += len(data)
                    sizes["serialized"] += len(data)
                    calls["native_archive_alias"] += 1
            return original_sha(data, *args, **kw)

        patch.setattr(matching.hashlib, "sha256", sha_spy)
        yield sizes, calls, trace


def assert_native_completion_counters(
    db: Any, source: Any, trace: Any, sizes: Any
) -> None:
    writes: Counter[str] = Counter()
    for statement, _, rowcount in trace:
        match = re.match(
            r'(?:INSERT\s+INTO|UPDATE)\s+"?(\w+)"?', statement.lstrip(), re.I
        )
        if match and rowcount > 0:
            writes[match[1]] += rowcount
    row = db.connection.execute(
        "SELECT group_state_write_count,claim_state_write_count,"
        "answer_state_write_count,certificate_binding_write_count,"
        "public_delta_count,bytes_hashed,bytes_serialized "
        "FROM groundloop_m5_runtime_work_contribution WHERE epoch_id=%s AND "
        "contribution_kind='verifier_completion' AND source_id=%s",
        (db.runtime.epoch_id, source.lease.attempt.attempt_id),
    ).fetchone()
    assert row == (
        writes["groundloop_m5_working_group_state"],
        writes["groundloop_m5_working_claim_state"],
        writes["groundloop_m5_working_answer_state"],
        writes["groundloop_m5_working_group_certificate_binding"]
        + writes["groundloop_m5_working_claim_certificate_binding"],
        0,
        sizes["hashed"],
        sizes["serialized"],
    ), (row, sizes)
    assert writes["groundloop_m5_runtime_work_contribution"] == 2
    assert writes["groundloop_m5_runtime_work_accumulator"] == 1
    assert "requirement_state_write_count" not in M5RuntimeWork.counter_names()


@pytest.mark.parametrize("c2_db", [None, "complete"], indirect=True)
@pytest.mark.parametrize("label", ["SUPPORT", "NEUTRAL", "REFUTE"])
def test_independent_native_producer_bytes_and_actual_physical_counters(
    c2_db: Any, label: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    db = c2_db
    source = db.build(db.chunks[0], label)
    with native_producer_observer(monkeypatch) as (sizes, calls, details):
        with capture_sql(db.connection) as trace:
            db.complete(source)
    assert calls["native_archive_alias"] == 15
    assert calls["m5-runtime-work-v2"] == 2
    try:
        assert_native_completion_counters(db, source, trace, sizes)
    except AssertionError as error:
        raise AssertionError((sizes, calls, [d for d in details if d[1]])) from error


@pytest.mark.parametrize("replacement", [False, True])
@pytest.mark.parametrize("c2_db", ["complete"], indirect=True)
def test_independent_bytes_for_existing_complete_certificate_changed_or_reused(
    c2_db: Any, replacement: bool, monkeypatch: pytest.MonkeyPatch
) -> None:
    from tests.m5.postgres_runtime.d34_requirement_completion import test_history as h

    db = c2_db
    first = db.build(db.chunks[0])
    db.complete(first)
    first_id = first.verifier_artifact.to_semantic_observation().observation_id
    candidates = (
        db.build(db.chunks[1], "SUPPORT", f"representative-{n}") for n in range(128)
    )
    source = next(
        value
        for value in candidates
        if (value.verifier_artifact.to_semantic_observation().observation_id < first_id)
        == replacement
    )
    old_digest = (
        db.connection.execute(
            "SELECT certificate_digest FROM groundloop_m5_working_group_state "
            "WHERE epoch_id=%s AND group_version_id=%s",
            (db.runtime.epoch_id, db.group.group_version_id),
        )
        .fetchone()[0]
        .strip()
    )
    immutable_before = db.connection.execute(
        "SELECT certificate_digest,xmin::text "
        "FROM groundloop_m5_group_certificate_artifact "
        "ORDER BY certificate_digest"
    ).fetchall()
    binding_before = db.connection.execute(
        "SELECT valid_from_revision,valid_to_revision,certificate_digest,xmin::text "
        "FROM groundloop_m5_working_group_certificate_binding "
        "WHERE epoch_id=%s ORDER BY valid_from_revision",
        (db.runtime.epoch_id,),
    ).fetchall()
    with native_producer_observer(monkeypatch) as (sizes, calls, details):
        with capture_sql(db.connection) as trace:
            db.complete(source)
    try:
        assert_native_completion_counters(db, source, trace, sizes)
    except AssertionError as error:
        raise AssertionError((sizes, calls, [d for d in details if d[1]])) from error
    digest = (
        db.connection.execute(
            "SELECT certificate_digest FROM groundloop_m5_working_group_state "
            "WHERE epoch_id=%s AND group_version_id=%s",
            (db.runtime.epoch_id, db.group.group_version_id),
        )
        .fetchone()[0]
        .strip()
    )
    assert (digest != old_digest) == replacement
    assert (
        db.connection.execute(
            "SELECT certificate_digest,xmin::text "
            "FROM groundloop_m5_group_certificate_artifact "
            "WHERE certificate_digest=ANY(%s) ORDER BY certificate_digest",
            ([r[0] for r in immutable_before],),
        ).fetchall()
        == immutable_before
    )
    if not replacement:
        assert (
            db.connection.execute(
                "SELECT valid_from_revision,valid_to_revision,"
                "certificate_digest,xmin::text "
                "FROM groundloop_m5_working_group_certificate_binding "
                "WHERE epoch_id=%s ORDER BY valid_from_revision",
                (db.runtime.epoch_id,),
            ).fetchall()
            == binding_before
        )
    expected = h.expected_currency_from_sources(db)
    h.assert_currency_sources_and_raw_history(db, expected)
    h._assert_complete_point(db, db.runtime.epoch_id, db.revision(), expected)


def test_independent_native_inactive_bytes_have_no_matching_producer(
    c2_db: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    db = c2_db
    source = db.build(db.chunks[2])
    with db.connection.transaction():
        db.connection.execute(
            "UPDATE groundloop_chunk_version SET valid_to_epoch=%s "
            "WHERE chunk_version_id=%s",
            (db.runtime.epoch_id, db.chunks[2]),
        )
        db.connection.execute("SET CONSTRAINTS ALL IMMEDIATE")
    with native_producer_observer(monkeypatch) as (sizes, calls, details):
        with capture_sql(db.connection) as trace:
            db.complete(source)
    assert calls["native_archive_alias"] == 15
    assert calls["native_absence"] == 0
    assert calls["m5-persisted-matching-transition-intent-v1"] == 0
    try:
        assert_native_completion_counters(db, source, trace, sizes)
    except AssertionError as error:
        raise AssertionError((sizes, calls, [d for d in details if d[1]])) from error


@pytest.mark.parametrize("prior_reported", [False, True])
@pytest.mark.parametrize("timing_mode", ["missing", "full", "partial"])
@pytest.mark.parametrize("magnitude", [9, 10, 99, 100])
def test_all_timing_coverage_and_final_work_decimal_widths(
    c2_db: Any,
    prior_reported: bool,
    timing_mode: str,
    magnitude: int,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from groundloop.m5.runtime import digests
    from groundloop.m5.runtime.contracts import M5RuntimeWorkContributionKind
    from groundloop.m5.runtime.persistence import PostgresM5RuntimeStore

    db = c2_db
    epoch = db.runtime.epoch_id
    pending = db.connection.execute(
        "SELECT pending_contribution_kind,pending_source_id,"
        "pending_contribution_key_digest,pending_anchor_revision FROM "
        "groundloop_m5_runtime_timing_accumulator WHERE epoch_id=%s",
        (epoch,),
    ).fetchone()
    assert pending[0] is not None
    if prior_reported:
        PostgresM5RuntimeStore(db.connection).append_transition_call_timing(
            epoch,
            M5RuntimeWorkContributionKind(pending[0]),
            pending[1],
            pending[2].strip(),
            pending[3],
            M5RuntimeTiming(1, 2, 3, 4, 10),
        )
    timing = {
        "missing": None,
        "full": M5RuntimeTiming(2, 3, 5, 7, 17, 11, 13, 19, 23),
        "partial": M5RuntimeTiming(2, 3, 5, 7, 17, 11, None, None, 23),
    }[timing_mode]
    source = db.build(db.chunks[0])
    work = M5RuntimeWork(bytes_hashed=magnitude, bytes_serialized=magnitude + 1)
    with native_producer_observer(monkeypatch) as (sizes, calls, details):
        with capture_sql(db.connection) as trace:
            receipt = db.complete(source, timing=timing, work=work)
    try:
        assert_native_completion_counters(db, source, trace, sizes)
    except AssertionError as error:
        raise AssertionError((sizes, calls, [d for d in details if d[1]])) from error
    assert calls["m5-runtime-work-v2"] == 2
    assert calls["m5-transition-call-timing-v1"] == int(not prior_reported)
    columns = (
        "coordinator_non_db_non_neural_ns",
        "neural_wall_ns",
        "postgres_roundtrip_wall_ns",
        "external_io_wall_ns",
        "end_to_end_wall_ns",
        "postgres_server_execution_ns",
        "postgres_lock_wait_ns",
        "postgres_wal_bytes",
        "postgres_shared_block_reads",
    )
    points = []
    for table in (
        "groundloop_m5_runtime_timing_contribution",
        "groundloop_m5_transition_call_timing",
    ):
        points.extend(
            db.connection.execute(
                "SELECT required_interval_observed,"
                + ",".join(columns)
                + f" FROM {table} WHERE epoch_id=%s",
                (epoch,),
            ).fetchall()
        )
    sums = tuple(sum(row[n + 1] or 0 for row in points) for n in range(9))
    coverage = []
    families = (
        "required",
        "postgres_server_execution",
        "postgres_lock_wait",
        "postgres_wal_bytes",
        "postgres_shared_block_reads",
    )
    for n in (0, 6, 7, 8, 9):
        observed = sum(bool(row[0]) if n == 0 else row[n] is not None for row in points)
        coverage.extend((len(points) + 1, observed, len(points) - observed))
    accumulator = db.connection.execute(
        "SELECT "
        + ",".join(columns)
        + ","
        + ",".join(
            f"{family}_{suffix}_count"
            for family in families
            for suffix in ("expected", "observed", "missing")
        )
        + ",pending_contribution_kind,pending_source_id,"
        "pending_contribution_key_digest,pending_anchor_revision,updated_revision "
        "FROM groundloop_m5_runtime_timing_accumulator WHERE epoch_id=%s",
        (epoch,),
    ).fetchone()
    assert tuple(accumulator[:9]) == sums
    assert tuple(accumulator[9:24]) == tuple(coverage)
    key = digests.runtime_work_contribution_key_digest(
        epoch_id=epoch,
        contribution_kind=M5RuntimeWorkContributionKind.VERIFIER_COMPLETION,
        source_id=source.lease.attempt.attempt_id,
    )
    assert tuple(accumulator[24:]) == (
        "verifier_completion",
        source.lease.attempt.attempt_id,
        key,
        receipt.resulting_revision,
        receipt.resulting_revision,
    )
    _, projected = PostgresM5RuntimeStore(db.connection).current_event_timing(epoch)
    assert tuple(
        getattr(projected, f"{family}_{suffix}_count")
        for family in families
        for suffix in ("expected", "observed", "missing")
    ) == tuple(value + int(n % 3 == 2) for n, value in enumerate(coverage))


@pytest.mark.parametrize("label", ["SUPPORT", "NEUTRAL", "REFUTE"])
@pytest.mark.parametrize("c2_db", [None, "complete"], indirect=True)
def test_native_reservation_certificate_and_final_work_producer_spies(
    c2_db: Any,
    label: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import inspect

    from groundloop.m5.runtime import digests
    from groundloop.m5.runtime import postgres_matching as matching
    from groundloop.m5.runtime import postgres_verifier as verifier

    db = c2_db
    source = db.build(db.chunks[0], label)
    regions: list[Any] = []
    reservations: list[Any] = []
    certificates: list[Any] = []
    final_work_preimages: list[bytes] = []
    phase = ["prefix"]
    reserve = matching._reserve_matching_absence

    def record_reservation(cursor: Any, relation: str, key: Any) -> Any:
        reservations.append((relation, key))
        return reserve(cursor, relation, key)

    native = verifier._d34_native_matching_regions

    def record_native(cursor: Any, prepared: Any) -> Any:
        values = native(cursor, prepared)
        regions.extend(values)
        return values

    source_regions = verifier._d34_source_regions

    def record_sources(*args: Any, **kwargs: Any) -> Any:
        values = source_regions(*args, **kwargs)
        regions.extend(values)
        return values

    reconstruct = matching.reconstruct_certificate

    def record_certificate(*args: Any, **kwargs: Any) -> Any:
        value = reconstruct(*args, **kwargs)
        if (
            phase[0] in {"verifier_execution_inserted", "d34_matching_derived"}
            and value.artifact is not None
        ):
            certificates.append(value.artifact)
        return value

    work_digest = digests.runtime_work_digest

    def record_work(counters: Any) -> Any:
        stack = inspect.stack()
        if any(frame.function == "_d34_complete_work" for frame in stack):
            final_work_preimages.append(
                digests.stable_m5_preimage(
                    "m5-runtime-work-v2",
                    *(verifier._d34_int_field(v) for v in counters),
                )
            )
        return work_digest(counters)

    monkeypatch.setattr(matching, "_reserve_matching_absence", record_reservation)
    monkeypatch.setattr(matching, "reconstruct_certificate", record_certificate)
    monkeypatch.setattr(verifier, "_d34_native_matching_regions", record_native)
    monkeypatch.setattr(verifier, "_d34_source_regions", record_sources)
    monkeypatch.setattr(digests, "runtime_work_digest", record_work)
    receipt = db.complete(
        source, failure_injector=lambda stage: phase.__setitem__(0, stage)
    )
    expected_reservation_bytes = sum(
        len(b"m5-d28-matching-absence-v1")
        + len(matching._matching_journal_key_preimage(relation, key))
        for relation, key in reservations
    )
    actual_reservation_regions = [
        r for r in regions if r.name.startswith("d25.absence_reservation.")
    ]
    assert len(actual_reservation_regions) == len(reservations)
    assert (
        sum(r.bytes_hashed for r in actual_reservation_regions)
        == expected_reservation_bytes
    )
    assert (
        sum(r.bytes_serialized for r in actual_reservation_regions)
        == expected_reservation_bytes
    )
    desired = [
        r for r in regions if r.name.startswith("d25.desired_group_certificate.")
    ]
    assert len(certificates) == (
        3
        if db.group.requirements
        and len(db.group.requirements) == 1
        and label == "SUPPORT"
        else 0
    )
    if certificates:
        assert len(desired) == 1
        assert desired[0].bytes_hashed == 3 * len(
            digests.stable_m5_preimage(
                "m5-group-certificate-v1",
                verifier._d34_text_field(certificates[0].decision_policy_version),
                verifier._d34_text_field(certificates[0].group_version_id),
                verifier._d34_int_field(len(certificates[0].rows)),
                verifier._d34_sequence_field(
                    verifier._d34_sequence_field(
                        (
                            verifier._d34_int_field(r.requirement_ordinal),
                            verifier._d34_text_field(r.requirement_version_id),
                            verifier._d34_hash_field(r.text_hash),
                            verifier._d34_text_field(r.selected_observation_id),
                        )
                    )
                    for r in certificates[0].rows
                ),
            )
        )
    else:
        assert desired == []
    assert len(final_work_preimages) == 2
    counters = db.connection.execute(
        "SELECT bytes_hashed,bytes_serialized FROM "
        "groundloop_m5_runtime_work_contribution "
        "WHERE epoch_id=%s AND contribution_kind='verifier_completion' AND "
        "source_id=%s",
        (db.runtime.epoch_id, source.lease.attempt.attempt_id),
    ).fetchone()
    assert counters == (
        sum(r.bytes_hashed for r in regions) + sum(map(len, final_work_preimages)),
        sum(r.bytes_serialized for r in regions) + sum(map(len, final_work_preimages)),
    )
    assert receipt.resulting_revision == db.revision()


@pytest.mark.parametrize("observed", [False, True])
def test_exact_contributions_single_cas_and_timing_coverage(
    c2_db: Any, observed: bool
) -> None:
    db = c2_db
    fixture = db.build(db.chunks[0])
    names = M5RuntimeWork.counter_names()
    before = db.connection.execute(
        "SELECT "
        + ",".join(names)
        + ",updated_revision FROM groundloop_m5_runtime_work_accumulator WHERE "
        "epoch_id=%s",
        (db.runtime.epoch_id,),
    ).fetchone()
    old_timing = db.connection.execute(
        "SELECT required_expected_count,required_observed_count,required_missing_count,"
        "pending_source_id FROM groundloop_m5_runtime_timing_accumulator WHERE "
        "epoch_id=%s",
        (db.runtime.epoch_id,),
    ).fetchone()
    timing = M5RuntimeTiming(2, 3, 5, 7, 17, 11, 13, 19, 23) if observed else None
    with capture_sql(db.connection) as trace:
        receipt = db.complete(fixture, timing=timing)
    contributions = db.connection.execute(
        "SELECT contribution_kind," + ",".join(names) + ",applied_revision "
        "FROM groundloop_m5_runtime_work_contribution WHERE epoch_id=%s AND "
        "source_id=%s "
        'ORDER BY contribution_kind COLLATE "C"',
        (db.runtime.epoch_id, fixture.lease.attempt.attempt_id),
    ).fetchall()
    assert [r[0] for r in contributions] == [
        "m5_attempt_execution",
        "verifier_completion",
    ]
    assert {r[-1] for r in contributions} == {receipt.resulting_revision}
    execution, completion = contributions
    e = dict(zip(names, execution[1:-1], strict=True))
    c = dict(zip(names, completion[1:-1], strict=True))
    assert e == dict(
        zip(
            names,
            M5RuntimeWork(bytes_hashed=41, bytes_serialized=43).counter_values(),
            strict=True,
        )
    )
    assert (
        c["requirement_observation_artifact_count"]
        == c["requirement_effective_observation_count"]
        == 1
    )
    assert c["requirement_inactive_completion_count"] == c["public_delta_count"] == 0
    assert c["requirement_verifier_call_count"] == c["verifier_model_call_count"] == 0
    assert c["bytes_hashed"] > 10_000 and c["bytes_serialized"] >= c["bytes_hashed"]
    after = db.connection.execute(
        "SELECT "
        + ",".join(names)
        + ",updated_revision FROM groundloop_m5_runtime_work_accumulator WHERE "
        "epoch_id=%s",
        (db.runtime.epoch_id,),
    ).fetchone()
    assert tuple(after[:-1]) == tuple(
        a + b + d
        for a, b, d in zip(before[:-1], execution[1:-1], completion[1:-1], strict=True)
    )
    assert after[-1] == receipt.resulting_revision
    statements = [s for s, _, _ in trace]
    updates = [
        s
        for s in statements
        if s.lstrip().startswith("UPDATE groundloop_m5_runtime_work_accumulator")
        or s.lstrip().startswith('UPDATE "groundloop_m5_runtime_work_accumulator"')
    ]
    assert len(updates) == 1
    timing_after = db.connection.execute(
        "SELECT required_expected_count,required_observed_count,required_missing_count,"
        "pending_source_id,pending_anchor_revision FROM "
        "groundloop_m5_runtime_timing_accumulator WHERE epoch_id=%s",
        (db.runtime.epoch_id,),
    ).fetchone()
    assert timing_after[:3] == (
        old_timing[0] + 2,
        old_timing[1] + int(observed),
        old_timing[2] + int(not observed) + int(old_timing[3] is not None),
    )
    assert timing_after[3:] == (
        fixture.lease.attempt.attempt_id,
        receipt.resulting_revision,
    )
    assert db.connection.execute(
        "SELECT required_interval_observed FROM "
        "groundloop_m5_runtime_timing_contribution "
        "WHERE epoch_id=%s AND attempt_id=%s",
        (db.runtime.epoch_id, fixture.lease.attempt.attempt_id),
    ).fetchone() == (observed,)

    def at(needle: str, *, mutation: bool = False) -> int:
        return next(
            i
            for i, (s, _, _) in enumerate(trace)
            if needle in s
            and (not mutation or s.lstrip().upper().startswith(("INSERT", "UPDATE")))
        )

    assert at("groundloop_m5_owner_pending_counter", mutation=True) < at(
        "groundloop_m5_answer_pending_counter", mutation=True
    )
    assert at("groundloop_m5_answer_pending_counter", mutation=True) < at(
        "groundloop_m5_runtime_work_contribution", mutation=True
    )
    assert at("groundloop_m5_runtime_work_contribution", mutation=True) < at(
        "groundloop_m5_runtime_work_accumulator", mutation=True
    )
    assert at("groundloop_m5_runtime_work_accumulator", mutation=True) < at(
        "groundloop_m5_runtime_timing_contribution", mutation=True
    )
    assert at("groundloop_m5_runtime_timing_contribution", mutation=True) < at(
        "groundloop_m5_runtime_timing_accumulator", mutation=True
    )
    assert at("groundloop_m5_runtime_timing_accumulator", mutation=True) < at(
        "groundloop_m5_matching_patch_artifact", mutation=True
    )


@pytest.mark.parametrize("c2_db", [None, "complete"], indirect=True)
@pytest.mark.parametrize("active", [False, True])
def test_every_actual_mutation_and_phase_rolls_back_reconnects_and_retries(
    c2_db: Any,
    active: bool,
) -> None:
    db = c2_db
    fixture = db.build(db.chunks[0] if active else db.chunks[2])
    if not active:
        with db.connection.transaction():
            db.connection.execute(
                "UPDATE groundloop_chunk_version SET valid_to_epoch=%s "
                "WHERE chunk_version_id=%s",
                (db.runtime.epoch_id, db.chunks[2]),
            )
            db.connection.execute("SET CONSTRAINTS ALL IMMEDIATE")
    before = database_snapshot(db.connection)
    phases: list[str] = []

    def discover(stage: str) -> None:
        phases.append(stage)
        if stage == "verifier_constraints_validated":
            raise RuntimeError("C2 phase discovery rollback")

    with capture_sql(db.connection) as trace:
        with pytest.raises(RuntimeError, match="phase discovery"):
            db.complete(fixture, failure_injector=discover)
    assert database_snapshot(db.connection) == before
    mutations = [
        r
        for r in trace
        if r[0].lstrip().upper().startswith(("INSERT", "UPDATE", "DELETE", "MERGE"))
        and r[2] > 0
    ]
    archive = [
        r
        for r in mutations
        if r[0].lstrip().startswith("INSERT")
        and any(
            name in r[0]
            for name in (
                "groundloop_m5_attempt_result_artifact (",
                "groundloop_m5_requirement_pair_input (",
                "groundloop_m5_requirement_verifier_artifact (",
                "groundloop_semantic_observation (",
                "groundloop_m5_requirement_verifier_execution (",
            )
        )
    ]
    assert len(archive) == 5 and len(mutations) >= (20 if active else 17)
    for number in range(1, len(mutations) + 1):
        with capture_sql(db.connection, fail_mutation=number):
            with pytest.raises(RuntimeError, match=f"actual mutation cut:{number}"):
                db.complete(fixture)
        with db.runtime.reconnect() as reader:
            assert database_snapshot(reader) == before
    for phase in phases:

        def cut(stage: str, selected: str = phase) -> None:
            if stage == selected:
                raise RuntimeError("C2 named phase cut")

        with pytest.raises(RuntimeError, match="named phase"):
            db.complete(fixture, failure_injector=cut)
        assert database_snapshot(db.connection) == before
    assert not db.complete(fixture).exact_replay
    with db.runtime.reconnect() as reader:
        assert db.complete(fixture, connection=reader).exact_replay
