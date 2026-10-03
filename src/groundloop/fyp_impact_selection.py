"""Bounded held-out diagnostic for text-only impact selection.

The selector ranks claims for a changed evidence item before neural
verification. Gold labels are used only after ranking to measure coverage.
"""

from __future__ import annotations

import csv
import hashlib
import io
import json
import re
import unicodedata
from collections import Counter
from dataclasses import asdict, dataclass
from difflib import SequenceMatcher
from enum import StrEnum
from fractions import Fraction
from pathlib import Path
from typing import Any, cast

from groundloop.errors import ValidationError
from groundloop.m4.contracts import stable_m4_digest

CONFIG_SCHEMA_VERSION = "groundloop-fyp-impact-selection-config-v1"
REPORT_SCHEMA_VERSION = "groundloop-fyp-impact-selection-report-v1"
SOURCE_ROW_SCHEMA_VERSION = "groundloop-m4-13-vitaminc-row-v1"
RESULT_SCOPE = "internal_held_out_selection_diagnostic_not_end_to_end_utility"
POLICY_IDS = (
    "changed_token_overlap",
    "new_evidence_overlap",
    "old_new_rarity_coverage",
)
LIMITATIONS = (
    "The source is a public VitaminC development artifact already used by M4.13, "
    "not an untouched external test set.",
    "The registry is constructed from two revision-sensitive claims per page; it "
    "is not a natural GroundLoop claim population.",
    "The diagnostic measures candidate-pair counts, not wall-clock latency, API "
    "cost, verifier accuracy, answer correctness, or end-to-end utility.",
    "The lexical selector is a bounded baseline, not a claim of novelty or "
    "superiority to another system.",
)

_TOKEN_RE = re.compile(r"[a-z0-9]+")
_CASE_SUFFIX_RE = re.compile(r"^(?P<case>.+)_(?P<position>[1-4])$")
_LOWER_HEX = frozenset("0123456789abcdef")
_SOURCE_KEYS = frozenset(
    {
        "case_id",
        "claim",
        "claim_sha256",
        "evidence",
        "evidence_sha256",
        "label",
        "normalized_page_sha256",
        "page",
        "revision_type",
        "schema_version",
        "selection_key",
        "source_label",
        "split",
        "stratum",
        "unique_id",
        "wiki_revision_id",
    }
)


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _sha256_text(value: str) -> str:
    return _sha256_bytes(value.encode("utf-8"))


def _require_sha256(name: str, value: str) -> None:
    if len(value) != 64 or any(character not in _LOWER_HEX for character in value):
        raise ValidationError(f"{name} must be a lowercase SHA-256 digest")


def _require_exact_keys(name: str, value: dict[str, Any], keys: frozenset[str]) -> None:
    if frozenset(value) != keys:
        raise ValidationError(f"{name} fields do not match the frozen schema")


def _require_positive_integer(name: str, value: object) -> int:
    if type(value) is not int or value <= 0:
        raise ValidationError(f"{name} must be a positive integer")
    return value


@dataclass(frozen=True, slots=True)
class RatioThreshold:
    numerator: int
    denominator: int

    def __post_init__(self) -> None:
        if type(self.numerator) is not int or type(self.denominator) is not int:
            raise ValidationError("ratio threshold values must be integers")
        if self.denominator <= 0 or not 0 <= self.numerator <= self.denominator:
            raise ValidationError("ratio threshold must be between zero and one")

    def accepts(self, numerator: int, denominator: int) -> bool:
        if denominator <= 0:
            return False
        return numerator * self.denominator >= self.numerator * denominator


@dataclass(frozen=True, slots=True)
class ImpactSelectionConfig:
    expected_source_sha256: str
    expected_source_schema_version: str
    expected_case_count: int
    strata: tuple[str, ...]
    cases_per_stratum: int
    development_cases_per_stratum: int
    policy_ids: tuple[str, ...]
    budgets: tuple[int, ...]
    minimum_affected_claim_recall: RatioThreshold
    minimum_full_event_coverage: RatioThreshold
    minimum_pair_work_reduction: RatioThreshold
    config_sha256: str

    def __post_init__(self) -> None:
        _require_sha256("expected_source_sha256", self.expected_source_sha256)
        _require_sha256("config_sha256", self.config_sha256)
        if self.expected_source_schema_version != SOURCE_ROW_SCHEMA_VERSION:
            raise ValidationError("unsupported source row schema")
        if self.expected_case_count != self.cases_per_stratum * len(self.strata):
            raise ValidationError("expected case and stratum counts disagree")
        if self.strata != tuple(sorted(set(self.strata))) or not self.strata:
            raise ValidationError("strata must be sorted, unique, and nonempty")
        if not 0 < self.development_cases_per_stratum < self.cases_per_stratum:
            raise ValidationError("development split must leave held-out cases")
        if self.policy_ids != POLICY_IDS:
            raise ValidationError("policy identities differ from the frozen order")
        if self.budgets != tuple(sorted(set(self.budgets))) or not self.budgets:
            raise ValidationError("budgets must be sorted, unique, and nonempty")
        if any(type(value) is not int or value <= 0 for value in self.budgets):
            raise ValidationError("budgets must be positive integers")


@dataclass(frozen=True, slots=True)
class ClaimCandidate:
    claim_sha256: str
    text: str

    def __post_init__(self) -> None:
        _require_sha256("claim_sha256", self.claim_sha256)
        if not self.text.strip() or _sha256_text(self.text) != self.claim_sha256:
            raise ValidationError("claim text does not match claim_sha256")


@dataclass(frozen=True, slots=True)
class RevisionCase:
    case_id: str
    selection_key: str
    stratum: str
    old_evidence: str
    new_evidence: str
    affected_claims: tuple[ClaimCandidate, ClaimCandidate]


@dataclass(frozen=True, slots=True)
class Partition:
    cases: tuple[RevisionCase, ...]
    registry: tuple[ClaimCandidate, ...]


@dataclass(frozen=True, slots=True)
class SelectionMetrics:
    policy_id: str
    budget: int
    event_count: int
    registry_claim_count: int
    exhaustive_pair_count: int
    selected_pair_count: int
    avoided_pair_count: int
    affected_claim_numerator: int
    affected_claim_denominator: int
    full_event_numerator: int
    full_event_denominator: int
    eligible: bool

    def __post_init__(self) -> None:
        if self.policy_id not in POLICY_IDS:
            raise ValidationError("unknown impact-selection policy")
        if self.budget <= 0 or self.event_count <= 0 or self.registry_claim_count <= 0:
            raise ValidationError("selection dimensions must be positive")
        if self.exhaustive_pair_count != self.event_count * self.registry_claim_count:
            raise ValidationError("exhaustive pair count is inconsistent")
        expected_selected = self.event_count * min(
            self.budget, self.registry_claim_count
        )
        if self.selected_pair_count != expected_selected:
            raise ValidationError("selected pair count is inconsistent")
        if self.avoided_pair_count != self.exhaustive_pair_count - expected_selected:
            raise ValidationError("avoided pair count is inconsistent")
        if not 0 <= self.affected_claim_numerator <= self.affected_claim_denominator:
            raise ValidationError("affected-claim metric is invalid")
        if not 0 <= self.full_event_numerator <= self.full_event_denominator:
            raise ValidationError("full-event metric is invalid")
        if self.affected_claim_denominator != self.event_count * 2:
            raise ValidationError("each event must contain exactly two affected claims")
        if self.full_event_denominator != self.event_count:
            raise ValidationError("full-event denominator is inconsistent")


@dataclass(frozen=True, slots=True)
class EvaluationEvent:
    event_index: int
    selected_claim_count: int
    affected_claim_count: int
    affected_selected_count: int
    full_coverage: bool


class ImpactSelectionVerdict(StrEnum):
    PASS = "PASS"
    FAIL = "FAIL"
    NO_CANDIDATE = "NO_CANDIDATE"


@dataclass(frozen=True, slots=True)
class ImpactSelectionReport:
    schema_version: str
    result_scope: str
    source_sha256: str
    config_sha256: str
    source_case_count: int
    source_row_count: int
    development_case_count: int
    evaluation_case_count: int
    registry_claim_count: int
    thresholds: dict[str, dict[str, int]]
    development_results: tuple[SelectionMetrics, ...]
    selected_policy_id: str | None
    selected_budget: int | None
    evaluation_result: SelectionMetrics | None
    evaluation_events: tuple[EvaluationEvent, ...]
    verdict: ImpactSelectionVerdict
    limitations: tuple[str, ...]

    @property
    def manifest_hash(self) -> str:
        return stable_m4_digest(
            REPORT_SCHEMA_VERSION,
            json.dumps(
                asdict(self),
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=False,
                allow_nan=False,
            ),
        )

    def to_canonical_json(self) -> str:
        payload = asdict(self)
        payload["report_manifest_hash"] = self.manifest_hash
        return (
            json.dumps(
                payload,
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=False,
                allow_nan=False,
            )
            + "\n"
        )

    def to_development_csv(self) -> str:
        fields = (
            "report_manifest_hash",
            "policy_id",
            "budget",
            "event_count",
            "registry_claim_count",
            "exhaustive_pair_count",
            "selected_pair_count",
            "avoided_pair_count",
            "affected_claim_numerator",
            "affected_claim_denominator",
            "full_event_numerator",
            "full_event_denominator",
            "eligible",
            "selected",
        )
        output = io.StringIO(newline="")
        writer = csv.DictWriter(output, fieldnames=fields, lineterminator="\n")
        writer.writeheader()
        for result in self.development_results:
            row = asdict(result)
            row["report_manifest_hash"] = self.manifest_hash
            row["eligible"] = str(result.eligible).lower()
            row["selected"] = str(
                result.policy_id == self.selected_policy_id
                and result.budget == self.selected_budget
            ).lower()
            writer.writerow(row)
        return output.getvalue()

    def to_evaluation_events_csv(self) -> str:
        fields = (
            "report_manifest_hash",
            "event_index",
            "selected_claim_count",
            "affected_claim_count",
            "affected_selected_count",
            "full_coverage",
        )
        output = io.StringIO(newline="")
        writer = csv.DictWriter(output, fieldnames=fields, lineterminator="\n")
        writer.writeheader()
        for event in self.evaluation_events:
            row = asdict(event)
            row["report_manifest_hash"] = self.manifest_hash
            row["full_coverage"] = str(event.full_coverage).lower()
            writer.writerow(row)
        return output.getvalue()


@dataclass(frozen=True, slots=True)
class ImpactSelectionBundle:
    report_path: Path
    development_csv_path: Path
    evaluation_events_csv_path: Path
    report_manifest_hash: str


def _load_ratio(value: object, name: str) -> RatioThreshold:
    if not isinstance(value, dict) or frozenset(value) != {"numerator", "denominator"}:
        raise ValidationError(f"{name} must contain numerator and denominator")
    return RatioThreshold(
        numerator=_require_positive_integer(f"{name}.numerator", value["numerator"]),
        denominator=_require_positive_integer(
            f"{name}.denominator", value["denominator"]
        ),
    )


def load_impact_selection_config(path: str | Path) -> ImpactSelectionConfig:
    config_bytes = Path(path).read_bytes()
    try:
        raw = json.loads(config_bytes)
    except json.JSONDecodeError as error:
        raise ValidationError("impact-selection config is not valid JSON") from error
    if not isinstance(raw, dict):
        raise ValidationError("impact-selection config must be an object")
    expected_keys = frozenset(
        {
            "schema_version",
            "expected_source_sha256",
            "expected_source_schema_version",
            "expected_case_count",
            "strata",
            "cases_per_stratum",
            "development_cases_per_stratum",
            "policy_ids",
            "budgets",
            "minimum_affected_claim_recall",
            "minimum_full_event_coverage",
            "minimum_pair_work_reduction",
        }
    )
    _require_exact_keys("config", raw, expected_keys)
    if raw["schema_version"] != CONFIG_SCHEMA_VERSION:
        raise ValidationError("unsupported impact-selection config schema")
    list_fields = ("strata", "policy_ids", "budgets")
    if any(not isinstance(raw[field], list) for field in list_fields):
        raise ValidationError("config list fields must be arrays")
    return ImpactSelectionConfig(
        expected_source_sha256=str(raw["expected_source_sha256"]),
        expected_source_schema_version=str(raw["expected_source_schema_version"]),
        expected_case_count=_require_positive_integer(
            "expected_case_count", raw["expected_case_count"]
        ),
        strata=tuple(str(value) for value in raw["strata"]),
        cases_per_stratum=_require_positive_integer(
            "cases_per_stratum", raw["cases_per_stratum"]
        ),
        development_cases_per_stratum=_require_positive_integer(
            "development_cases_per_stratum",
            raw["development_cases_per_stratum"],
        ),
        policy_ids=tuple(str(value) for value in raw["policy_ids"]),
        budgets=tuple(
            _require_positive_integer("budget", value) for value in raw["budgets"]
        ),
        minimum_affected_claim_recall=_load_ratio(
            raw["minimum_affected_claim_recall"],
            "minimum_affected_claim_recall",
        ),
        minimum_full_event_coverage=_load_ratio(
            raw["minimum_full_event_coverage"],
            "minimum_full_event_coverage",
        ),
        minimum_pair_work_reduction=_load_ratio(
            raw["minimum_pair_work_reduction"],
            "minimum_pair_work_reduction",
        ),
        config_sha256=_sha256_bytes(config_bytes),
    )


def _required_text(row: dict[str, Any], field: str) -> str:
    value = row[field]
    if not isinstance(value, str) or not value.strip():
        raise ValidationError(f"source {field} must be nonempty text")
    return value


def _validate_source_row(value: object) -> dict[str, str]:
    if not isinstance(value, dict):
        raise ValidationError("source row must be an object")
    row = cast(dict[str, Any], value)
    _require_exact_keys("source row", row, _SOURCE_KEYS)
    text_row = {field: _required_text(row, field) for field in _SOURCE_KEYS}
    if text_row["schema_version"] != SOURCE_ROW_SCHEMA_VERSION:
        raise ValidationError("source row schema differs from the frozen schema")
    if text_row["split"] != "development" or text_row["revision_type"] != "real":
        raise ValidationError("source row is outside the frozen population")
    if _sha256_text(text_row["claim"]) != text_row["claim_sha256"]:
        raise ValidationError("source claim hash mismatch")
    if _sha256_text(text_row["evidence"]) != text_row["evidence_sha256"]:
        raise ValidationError("source evidence hash mismatch")
    for field in (
        "claim_sha256",
        "evidence_sha256",
        "normalized_page_sha256",
        "selection_key",
    ):
        _require_sha256(field, text_row[field])
    return text_row


def _build_case(rows: list[dict[str, str]]) -> RevisionCase:
    if len(rows) != 4:
        raise ValidationError("each source case must contain exactly four rows")
    by_position: dict[int, dict[str, str]] = {}
    case_id = rows[0]["case_id"]
    for row in rows:
        match = _CASE_SUFFIX_RE.fullmatch(row["unique_id"])
        if match is None or match.group("case") != case_id:
            raise ValidationError("source unique_id does not match case identity")
        position = int(match.group("position"))
        if position in by_position:
            raise ValidationError("source case contains a duplicate row position")
        by_position[position] = row
    if frozenset(by_position) != {1, 2, 3, 4}:
        raise ValidationError("source case positions must be exactly 1 through 4")
    ordered = tuple(by_position[index] for index in range(1, 5))
    invariant_fields = (
        "case_id",
        "normalized_page_sha256",
        "page",
        "selection_key",
        "stratum",
    )
    for field in invariant_fields:
        if len({row[field] for row in ordered}) != 1:
            raise ValidationError(f"source case has inconsistent {field}")
    first, second, third, fourth = ordered
    if (first["claim"], first["claim_sha256"]) != (
        second["claim"],
        second["claim_sha256"],
    ) or (third["claim"], third["claim_sha256"]) != (
        fourth["claim"],
        fourth["claim_sha256"],
    ):
        raise ValidationError("source case claim pairing is invalid")
    if (first["evidence"], first["evidence_sha256"]) != (
        third["evidence"],
        third["evidence_sha256"],
    ) or (second["evidence"], second["evidence_sha256"]) != (
        fourth["evidence"],
        fourth["evidence_sha256"],
    ):
        raise ValidationError("source case evidence pairing is invalid")
    if first["claim_sha256"] == third["claim_sha256"]:
        raise ValidationError("source case claims must be distinct")
    if first["evidence_sha256"] == second["evidence_sha256"]:
        raise ValidationError("source case old and new evidence must differ")
    if first["label"] == second["label"] or third["label"] == fourth["label"]:
        raise ValidationError("source case claims must change gold label")
    return RevisionCase(
        case_id=case_id,
        selection_key=first["selection_key"],
        stratum=first["stratum"],
        old_evidence=first["evidence"],
        new_evidence=second["evidence"],
        affected_claims=(
            ClaimCandidate(first["claim_sha256"], first["claim"]),
            ClaimCandidate(third["claim_sha256"], third["claim"]),
        ),
    )


def load_revision_cases(
    source_path: str | Path, config: ImpactSelectionConfig
) -> tuple[RevisionCase, ...]:
    source_bytes = Path(source_path).read_bytes()
    if _sha256_bytes(source_bytes) != config.expected_source_sha256:
        raise ValidationError("source SHA-256 differs from the frozen artifact")
    groups: dict[str, list[dict[str, str]]] = {}
    for line_number, line in enumerate(source_bytes.splitlines(), start=1):
        if not line.strip():
            raise ValidationError(f"source row {line_number} is empty")
        try:
            value = json.loads(line)
        except json.JSONDecodeError as error:
            message = f"source row {line_number} is invalid JSON"
            raise ValidationError(message) from error
        row = _validate_source_row(value)
        if row["schema_version"] != config.expected_source_schema_version:
            raise ValidationError("source schema does not match config")
        groups.setdefault(row["case_id"], []).append(row)
    if len(groups) != config.expected_case_count:
        raise ValidationError("source case count differs from the frozen population")
    cases = tuple(_build_case(rows) for _, rows in sorted(groups.items()))
    by_stratum = Counter(case.stratum for case in cases)
    if set(by_stratum) != set(config.strata) or any(
        by_stratum[stratum] != config.cases_per_stratum for stratum in config.strata
    ):
        raise ValidationError("source stratum counts differ from the frozen population")
    all_claims = [
        claim.claim_sha256 for case in cases for claim in case.affected_claims
    ]
    if len(all_claims) != len(set(all_claims)):
        raise ValidationError("source population contains duplicate claims")
    return cases


def split_revision_cases(
    cases: tuple[RevisionCase, ...], config: ImpactSelectionConfig
) -> tuple[Partition, Partition]:
    development: list[RevisionCase] = []
    evaluation: list[RevisionCase] = []
    for stratum in config.strata:
        members = sorted(
            (case for case in cases if case.stratum == stratum),
            key=lambda case: (case.selection_key, case.case_id),
        )
        cut = config.development_cases_per_stratum
        development.extend(members[:cut])
        evaluation.extend(members[cut:])

    def partition(values: list[RevisionCase]) -> Partition:
        ordered_cases = tuple(
            sorted(values, key=lambda case: (case.selection_key, case.case_id))
        )
        registry = tuple(
            sorted(
                (claim for case in ordered_cases for claim in case.affected_claims),
                key=lambda claim: claim.claim_sha256,
            )
        )
        return Partition(cases=ordered_cases, registry=registry)

    return partition(development), partition(evaluation)


def _tokens(value: str) -> tuple[str, ...]:
    normalized = unicodedata.normalize("NFKC", value).casefold()
    return tuple(_TOKEN_RE.findall(normalized))


def _overlap(left: tuple[str, ...], right: tuple[str, ...]) -> int:
    return sum((Counter(left) & Counter(right)).values())


def _changed_new_tokens(
    old_tokens: tuple[str, ...], new_tokens: tuple[str, ...]
) -> tuple[str, ...]:
    changed: list[str] = []
    matcher = SequenceMatcher(a=old_tokens, b=new_tokens, autojunk=False)
    for tag, _, _, start, stop in matcher.get_opcodes():
        if tag in {"insert", "replace"}:
            changed.extend(new_tokens[start:stop])
    return tuple(changed)


def rank_claims(
    *,
    policy_id: str,
    claims: tuple[ClaimCandidate, ...],
    old_evidence: str,
    new_evidence: str,
) -> tuple[ClaimCandidate, ...]:
    """Rank candidates without access to identities or gold labels."""
    if policy_id not in POLICY_IDS:
        raise ValidationError("unknown impact-selection policy")
    old_tokens = _tokens(old_evidence)
    new_tokens = _tokens(new_evidence)
    changed_tokens = _changed_new_tokens(old_tokens, new_tokens)
    claim_tokens = {
        claim.claim_sha256: _tokens(claim.text) for claim in claims
    }
    document_frequency: Counter[str] = Counter()
    for tokens in claim_tokens.values():
        document_frequency.update(set(tokens))

    def rarity_coverage(
        tokens: tuple[str, ...], evidence_tokens: tuple[str, ...]
    ) -> Fraction:
        unique_tokens = set(tokens)
        weights = {
            token: max(1, len(claims) // document_frequency[token])
            for token in unique_tokens
        }
        denominator = sum(weights.values())
        evidence_set = set(evidence_tokens)
        numerator = sum(
            weight for token, weight in weights.items() if token in evidence_set
        )
        return Fraction(numerator, denominator)

    def score(claim: ClaimCandidate) -> tuple[Fraction, Fraction]:
        tokens = claim_tokens[claim.claim_sha256]
        new_overlap = _overlap(tokens, new_tokens)
        if policy_id == "new_evidence_overlap":
            return Fraction(new_overlap), Fraction()
        if policy_id == "changed_token_overlap":
            return Fraction(_overlap(tokens, changed_tokens)), Fraction(new_overlap)
        old_coverage = rarity_coverage(tokens, old_tokens)
        new_coverage = rarity_coverage(tokens, new_tokens)
        return max(old_coverage, new_coverage), min(old_coverage, new_coverage)

    return tuple(
        sorted(
            claims,
            key=lambda claim: (-score(claim)[0], -score(claim)[1], claim.claim_sha256),
        )
    )


def _is_eligible(
    metrics: SelectionMetrics, config: ImpactSelectionConfig
) -> bool:
    return (
        config.minimum_affected_claim_recall.accepts(
            metrics.affected_claim_numerator, metrics.affected_claim_denominator
        )
        and config.minimum_full_event_coverage.accepts(
            metrics.full_event_numerator, metrics.full_event_denominator
        )
        and config.minimum_pair_work_reduction.accepts(
            metrics.avoided_pair_count, metrics.exhaustive_pair_count
        )
    )


def evaluate_policy(
    *,
    partition: Partition,
    policy_id: str,
    budget: int,
    config: ImpactSelectionConfig,
) -> tuple[SelectionMetrics, tuple[EvaluationEvent, ...]]:
    if budget <= 0:
        raise ValidationError("selection budget must be positive")
    affected_selected = 0
    fully_covered = 0
    events: list[EvaluationEvent] = []
    selected_count = min(budget, len(partition.registry))
    for event_index, case in enumerate(partition.cases):
        ranked = rank_claims(
            policy_id=policy_id,
            claims=partition.registry,
            old_evidence=case.old_evidence,
            new_evidence=case.new_evidence,
        )
        selected = {claim.claim_sha256 for claim in ranked[:selected_count]}
        affected = {claim.claim_sha256 for claim in case.affected_claims}
        overlap_count = len(selected & affected)
        full_coverage = overlap_count == len(affected)
        affected_selected += overlap_count
        fully_covered += int(full_coverage)
        events.append(
            EvaluationEvent(
                event_index=event_index,
                selected_claim_count=selected_count,
                affected_claim_count=len(affected),
                affected_selected_count=overlap_count,
                full_coverage=full_coverage,
            )
        )
    event_count = len(partition.cases)
    exhaustive = event_count * len(partition.registry)
    selected_pairs = event_count * selected_count
    provisional = SelectionMetrics(
        policy_id=policy_id,
        budget=budget,
        event_count=event_count,
        registry_claim_count=len(partition.registry),
        exhaustive_pair_count=exhaustive,
        selected_pair_count=selected_pairs,
        avoided_pair_count=exhaustive - selected_pairs,
        affected_claim_numerator=affected_selected,
        affected_claim_denominator=event_count * 2,
        full_event_numerator=fully_covered,
        full_event_denominator=event_count,
        eligible=False,
    )
    metrics = SelectionMetrics(
        **{
            **asdict(provisional),
            "eligible": _is_eligible(provisional, config),
        }
    )
    return metrics, tuple(events)


def _select_development_candidate(
    results: tuple[SelectionMetrics, ...],
) -> SelectionMetrics | None:
    eligible = [result for result in results if result.eligible]
    if not eligible:
        return None

    def order(
        result: SelectionMetrics,
    ) -> tuple[int, Fraction, Fraction, Fraction, str]:
        return (
            result.budget,
            -Fraction(
                result.affected_claim_numerator,
                result.affected_claim_denominator,
            ),
            -Fraction(result.full_event_numerator, result.full_event_denominator),
            -Fraction(result.avoided_pair_count, result.exhaustive_pair_count),
            result.policy_id,
        )

    return min(eligible, key=order)


def run_impact_selection(
    *, config_path: str | Path, source_path: str | Path
) -> ImpactSelectionReport:
    config = load_impact_selection_config(config_path)
    cases = load_revision_cases(source_path, config)
    development, evaluation = split_revision_cases(cases, config)
    development_results = tuple(
        evaluate_policy(
            partition=development,
            policy_id=policy_id,
            budget=budget,
            config=config,
        )[0]
        for policy_id in config.policy_ids
        for budget in config.budgets
    )
    selected = _select_development_candidate(development_results)
    evaluation_result: SelectionMetrics | None = None
    evaluation_events: tuple[EvaluationEvent, ...] = ()
    if selected is not None:
        evaluation_result, evaluation_events = evaluate_policy(
            partition=evaluation,
            policy_id=selected.policy_id,
            budget=selected.budget,
            config=config,
        )
    verdict = (
        ImpactSelectionVerdict.NO_CANDIDATE
        if selected is None
        else (
            ImpactSelectionVerdict.PASS
            if evaluation_result is not None and evaluation_result.eligible
            else ImpactSelectionVerdict.FAIL
        )
    )
    threshold_payload = {
        "affected_claim_recall": asdict(config.minimum_affected_claim_recall),
        "full_event_coverage": asdict(config.minimum_full_event_coverage),
        "pair_work_reduction": asdict(config.minimum_pair_work_reduction),
    }
    return ImpactSelectionReport(
        schema_version=REPORT_SCHEMA_VERSION,
        result_scope=RESULT_SCOPE,
        source_sha256=config.expected_source_sha256,
        config_sha256=config.config_sha256,
        source_case_count=len(cases),
        source_row_count=len(cases) * 4,
        development_case_count=len(development.cases),
        evaluation_case_count=len(evaluation.cases),
        registry_claim_count=len(evaluation.registry),
        thresholds=threshold_payload,
        development_results=development_results,
        selected_policy_id=None if selected is None else selected.policy_id,
        selected_budget=None if selected is None else selected.budget,
        evaluation_result=evaluation_result,
        evaluation_events=evaluation_events,
        verdict=verdict,
        limitations=LIMITATIONS,
    )


def write_impact_selection_bundle(
    report: ImpactSelectionReport, output_directory: str | Path
) -> ImpactSelectionBundle:
    output = Path(output_directory)
    output.mkdir(parents=True, exist_ok=True)
    report_path = output / "impact_selection_report.json"
    development_path = output / "development_frontier.csv"
    events_path = output / "evaluation_events.csv"
    report_path.write_text(report.to_canonical_json(), encoding="utf-8")
    development_path.write_text(report.to_development_csv(), encoding="utf-8")
    events_path.write_text(report.to_evaluation_events_csv(), encoding="utf-8")
    return ImpactSelectionBundle(
        report_path=report_path,
        development_csv_path=development_path,
        evaluation_events_csv_path=events_path,
        report_manifest_hash=report.manifest_hash,
    )


def execute_impact_selection(
    *, config_path: str | Path, source_path: str | Path, output_directory: str | Path
) -> tuple[ImpactSelectionReport, ImpactSelectionBundle]:
    report = run_impact_selection(config_path=config_path, source_path=source_path)
    return report, write_impact_selection_bundle(report, output_directory)


def _fraction(numerator: int, denominator: int) -> str:
    return f"{numerator}/{denominator} ({100 * numerator / denominator:.1f}%)"


def format_impact_selection_summary(
    report: ImpactSelectionReport, *, output_directory: str | Path
) -> str:
    lines = [
        "GroundLoop FYP impact-selection diagnostic",
        f"Verdict: {report.verdict.value}",
        f"Development/evaluation cases: {report.development_case_count}/"
        f"{report.evaluation_case_count}",
    ]
    result = report.evaluation_result
    if result is None:
        lines.append("Finding: no development candidate met the frozen thresholds.")
    else:
        avoided_percent = (
            100 * result.avoided_pair_count / result.exhaustive_pair_count
        )
        lines.extend(
            (
                f"Selected: {result.policy_id} at budget {result.budget}",
                "Held-out affected-claim recall: "
                + _fraction(
                    result.affected_claim_numerator,
                    result.affected_claim_denominator,
                ),
                "Held-out full-event coverage: "
                + _fraction(result.full_event_numerator, result.full_event_denominator),
                "Pair work: "
                f"{result.selected_pair_count}/{result.exhaustive_pair_count} "
                f"({avoided_percent:.1f}% "
                "avoided)",
            )
        )
    lines.extend(
        (
            f"Artifacts: {Path(output_directory)}",
            f"Report hash: {report.manifest_hash}",
            "Boundary: internal lexical selection diagnostic only; no LLM, "
            "latency, cost, answer-quality, or representative-utility claim.",
        )
    )
    return "\n".join(lines)
