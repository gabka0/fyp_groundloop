"""Differential and adversarial tests for independent optimized maintenance."""

from copy import deepcopy

from helpers import STAMP, insert_document, make_observation, make_repo, register_answer

from groundloop.domain import (
    ClaimStatus,
    DecisionPolicy,
    SemanticObservation,
    SubjectKind,
)
from groundloop.events import (
    DeleteDocumentVersionEvent,
    Event,
    ObserveEvent,
    PolicyChangeEvent,
    apply_event,
)
from groundloop.optimized.engine import ExactFlipMaintenanceEngine
from groundloop.reference import compute_all_states
from groundloop.repository import InMemoryRepository


def _commit(
    engine: ExactFlipMaintenanceEngine,
    repo: InMemoryRepository,
    event: Event,
) -> None:
    before = deepcopy(repo)
    apply_event(repo, event)
    engine.apply_committed_event(event, before, repo)
    expected_claims, expected_answers = compute_all_states(repo)
    assert engine.claim_states == expected_claims
    assert engine.answer_states == expected_answers
    engine.validate()


def _populate_interval_workload(
    *, neutral_count: int, flipping_count: int, duplicate_text: bool = False
) -> tuple[InMemoryRepository, ExactFlipMaintenanceEngine]:
    repo = make_repo(support_threshold=0.8)
    register_answer(repo)
    chunks: list[tuple[str, str]] = []
    for index in range(neutral_count + flipping_count):
        text = "duplicate" if duplicate_text else f"evidence-{index}"
        chunks.append((f"p-{index}", text))
    insert_document(repo, "insert", "doc", "dv", tuple(chunks))
    for index in range(neutral_count):
        apply_event(
            repo,
            ObserveEvent(
                f"neutral-event-{index}",
                make_observation(
                    f"neutral-{index}", "c1", f"p-{index}", (0.7, 0.1, 0.95)
                ),
            ),
        )
    for offset in range(flipping_count):
        index = neutral_count + offset
        apply_event(
            repo,
            ObserveEvent(
                f"flip-event-{offset}",
                make_observation(
                    f"flip-{offset}", "c1", f"p-{index}", (0.7, 0.1, 0.2)
                ),
            ),
        )
    return repo, ExactFlipMaintenanceEngine.from_repository(repo)


def test_selective_partition_visits_flips_not_raw_score_interval() -> None:
    repo, engine = _populate_interval_workload(
        neutral_count=500, flipping_count=3
    )
    _commit(
        engine,
        repo,
        PolicyChangeEvent("lower", DecisionPolicy("k-lower", 0.6, 0.8)),
    )
    assert engine.last_stats.policy_index_searches == 1
    assert engine.last_stats.policy_index_candidates == 3
    assert engine.last_stats.decision_flips == 3
    # A raw support-score interval contains all 503 rows; neutral dominance
    # proves why the potential partition is a material selectivity gain.
    assert 503 > engine.last_stats.policy_index_candidates


def test_f_zero_and_dense_flip_regimes_are_reported_exactly() -> None:
    zero_repo, zero_engine = _populate_interval_workload(
        neutral_count=200, flipping_count=0
    )
    _commit(
        zero_engine,
        zero_repo,
        PolicyChangeEvent("zero", DecisionPolicy("k-zero", 0.6, 0.8)),
    )
    assert zero_engine.last_stats.policy_index_candidates == 0

    dense_repo, dense_engine = _populate_interval_workload(
        neutral_count=0, flipping_count=200
    )
    _commit(
        dense_engine,
        dense_repo,
        PolicyChangeEvent("dense", DecisionPolicy("k-dense", 0.6, 0.8)),
    )
    assert dense_engine.last_stats.policy_index_candidates == 200
    assert dense_engine.last_stats.decision_flips == 200


def test_duplicate_content_multiplicity_flips_rows_but_crosses_one_hash() -> None:
    repo, engine = _populate_interval_workload(
        neutral_count=0, flipping_count=100, duplicate_text=True
    )
    _commit(
        engine,
        repo,
        PolicyChangeEvent("duplicate", DecisionPolicy("k-duplicate", 0.6, 0.8)),
    )
    assert engine.last_stats.decision_flips == 100
    assert engine.last_stats.distinct_hash_crossings == 1
    state = engine.claim_states["c1"]
    assert state.support_count == 1
    assert state.status is ClaimStatus.SUPPORTED


def test_supersession_storm_keeps_one_current_index_entry_and_valid_state() -> None:
    repo = make_repo(support_threshold=0.5, refute_threshold=0.5)
    register_answer(repo)
    insert_document(repo, "insert", "doc", "dv", (("p", "evidence"),))
    engine = ExactFlipMaintenanceEngine.from_repository(repo)
    for index in range(250):
        if index % 3 == 0:
            scores = (0.9, 0.05, 0.05)
        elif index % 3 == 1:
            scores = (0.05, 0.9, 0.05)
        else:
            scores = (0.05, 0.05, 0.9)
        _commit(
            engine,
            repo,
            ObserveEvent(
                f"supersede-event-{index}",
                SemanticObservation(
                    observation_id=f"o-{index}",
                    subject_kind=SubjectKind.CLAIM,
                    subject_id="c1",
                    chunk_version_id="p",
                    task_type="verify",
                    support_score=scores[0],
                    refute_score=scores[1],
                    neutral_score=scores[2],
                    producer=STAMP,
                    input_hash=f"input-{index}",
                ),
            ),
        )
    assert engine.indexed_potential_observations in {0, 1}


def test_document_withdrawal_reports_k_theta_e_point_removals() -> None:
    count = 200
    repo = make_repo(support_threshold=0.5)
    register_answer(repo)
    chunks = tuple((f"p-{index}", f"evidence-{index}") for index in range(count))
    insert_document(repo, "insert", "doc", "dv", chunks)
    for index in range(count):
        apply_event(
            repo,
            ObserveEvent(
                f"observe-{index}",
                make_observation(
                    f"o-{index}", "c1", f"p-{index}", (0.9, 0.05, 0.05)
                ),
            ),
        )
    engine = ExactFlipMaintenanceEngine.from_repository(repo)
    _commit(engine, repo, DeleteDocumentVersionEvent("delete", "dv"))
    assert engine.last_stats.active_observations_removed == count
    assert engine.last_stats.point_index_updates == count
    assert engine.claim_states["c1"].status is ClaimStatus.UNSUPPORTED


def test_exact_event_replay_is_an_optimized_no_op() -> None:
    repo = make_repo()
    register_answer(repo)
    insert_document(repo, "insert", "doc", "dv", (("p", "evidence"),))
    engine = ExactFlipMaintenanceEngine.from_repository(repo)
    event = ObserveEvent(
        "observe", make_observation("o", "c1", "p", (0.9, 0.05, 0.05))
    )
    _commit(engine, repo, event)
    before = deepcopy(engine)
    _commit(engine, repo, event)
    assert engine.claim_states == before.claim_states
    assert engine.last_stats.decision_flips == 0
