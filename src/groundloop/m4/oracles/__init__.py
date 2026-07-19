"""Independent empirical baselines for GroundLoop M4.

These modules deliberately do not depend on selective admission, runtime
maintenance, or the M1/M2 incremental paths. They consume raw claims, chunks,
vectors, and immutable pair judgments.
"""

from groundloop.m4.oracles.affected import (
    compute_affected_sets,
    pair_positive_claim_ids,
)
from groundloop.m4.oracles.evaluation import (
    FROZEN_HISTORY_BOOTSTRAP_V1,
    AlignedEventPair,
    BootstrapConfig,
    EvaluationProvenance,
    EventMetricRecord,
    MetricCount,
    MetricName,
    MetricUnit,
    OracleKind,
    PairedBootstrapResult,
    PairedPolicyRun,
    PolicyMetricSummary,
    PolicyRun,
    align_paired_policy_records,
    paired_history_cluster_bootstrap,
    summarize_policy_metric,
    validate_policy_run,
)
from groundloop.m4.oracles.exhaustive_delta import (
    ExhaustiveAdditiveDelta,
    compute_exhaustive_additive_delta,
)
from groundloop.m4.oracles.full_pair import run_full_pair_audit
from groundloop.m4.oracles.grounding import recompute_grounding_states
from groundloop.m4.oracles.refresh import (
    RefreshChunk,
    RefreshClaim,
    exact_brute_force_pairs,
    run_snapshot_refresh,
)
from groundloop.m4.oracles.reporting import (
    AnswerStatusResult,
    ClaimStatusResult,
    OracleEventResult,
    PairedEvaluationReport,
    PairedEventDiagnostic,
    TreatmentEventResult,
    build_paired_evaluation_report,
    derive_event_metric_record,
)
from groundloop.m4.oracles.testing import DeterministicJudgmentTable, PairJudge
from groundloop.m4.oracles.workload import (
    ControlledEventSpec,
    ControlledHistorySpec,
    ControlledWorkload,
    WorkloadSplit,
    build_controlled_dynamic_workload_v1,
)

__all__ = [
    "DeterministicJudgmentTable",
    "AlignedEventPair",
    "AnswerStatusResult",
    "BootstrapConfig",
    "ClaimStatusResult",
    "ControlledEventSpec",
    "ControlledHistorySpec",
    "ControlledWorkload",
    "EvaluationProvenance",
    "EventMetricRecord",
    "FROZEN_HISTORY_BOOTSTRAP_V1",
    "ExhaustiveAdditiveDelta",
    "MetricCount",
    "MetricName",
    "MetricUnit",
    "OracleEventResult",
    "OracleKind",
    "PairedBootstrapResult",
    "PairedEvaluationReport",
    "PairedEventDiagnostic",
    "PairedPolicyRun",
    "PairJudge",
    "PolicyMetricSummary",
    "PolicyRun",
    "RefreshChunk",
    "RefreshClaim",
    "TreatmentEventResult",
    "WorkloadSplit",
    "compute_affected_sets",
    "compute_exhaustive_additive_delta",
    "exact_brute_force_pairs",
    "pair_positive_claim_ids",
    "align_paired_policy_records",
    "build_controlled_dynamic_workload_v1",
    "build_paired_evaluation_report",
    "derive_event_metric_record",
    "paired_history_cluster_bootstrap",
    "recompute_grounding_states",
    "run_full_pair_audit",
    "run_snapshot_refresh",
    "summarize_policy_metric",
    "validate_policy_run",
]
