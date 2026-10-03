"""Provider-neutral request identities and OpenAI Batch request planning.

This module builds request bytes and cost bounds. Network execution is a
separate explicit stage so ordinary tests and offline reports cannot spend.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass
from decimal import ROUND_CEILING, Decimal, InvalidOperation
from pathlib import Path
from typing import Any, cast

from groundloop.errors import ValidationError
from groundloop.fyp_impact_selection import Partition
from groundloop.m4.contracts import stable_m4_digest

CONFIG_SCHEMA_VERSION = "groundloop-hosted-verifier-config-v1"
REQUEST_SCHEMA_VERSION = "groundloop-hosted-verifier-request-v1"
MANIFEST_SCHEMA_VERSION = "groundloop-hosted-verifier-request-manifest-v1"
_LOWER_HEX = frozenset("0123456789abcdef")


def _canonical_json(value: object) -> str:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    )


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _sha256_text(value: str) -> str:
    return _sha256_bytes(value.encode("utf-8"))


def _require_sha256(name: str, value: str) -> None:
    if len(value) != 64 or any(character not in _LOWER_HEX for character in value):
        raise ValidationError(f"{name} must be a lowercase SHA-256 digest")


def _decimal(value: object, name: str) -> Decimal:
    if not isinstance(value, str):
        raise ValidationError(f"{name} must be a decimal string")
    try:
        parsed = Decimal(value)
    except InvalidOperation as error:
        raise ValidationError(f"{name} is not a decimal") from error
    if not parsed.is_finite() or parsed < 0:
        raise ValidationError(f"{name} must be finite and nonnegative")
    return parsed


def _positive_integer(value: object, name: str) -> int:
    if type(value) is not int or value <= 0:
        raise ValidationError(f"{name} must be a positive integer")
    return value


@dataclass(frozen=True, slots=True)
class HostedVerifierConfig:
    provider: str
    model: str
    endpoint: str
    reasoning_effort: str
    max_output_tokens: int
    standard_input_usd_per_million: Decimal
    standard_output_usd_per_million: Decimal
    batch_discount_numerator: int
    batch_discount_denominator: int
    maximum_program_spend_usd: Decimal
    smoke_request_limit: int
    smoke_spend_limit_usd: Decimal
    prompt_version: str
    instructions: str
    output_schema: dict[str, object]
    config_sha256: str

    def __post_init__(self) -> None:
        if self.provider != "openai":
            raise ValidationError("Task 5C currently freezes the OpenAI provider")
        if self.model != "gpt-6-luna" or self.endpoint != "/v1/responses":
            raise ValidationError("hosted verifier model or endpoint drifted")
        if self.reasoning_effort != "none":
            raise ValidationError("hosted verifier must disable reasoning effort")
        _positive_integer(self.max_output_tokens, "max_output_tokens")
        _positive_integer(
            self.batch_discount_numerator, "batch_discount_numerator"
        )
        _positive_integer(
            self.batch_discount_denominator, "batch_discount_denominator"
        )
        if self.batch_discount_numerator >= self.batch_discount_denominator:
            raise ValidationError("Batch discount must be strictly below standard")
        if not self.prompt_version.strip() or not self.instructions.strip():
            raise ValidationError("prompt identity and instructions must be nonempty")
        _require_sha256("config_sha256", self.config_sha256)
        expected_schema = {
            "type": "object",
            "properties": {
                "label": {
                    "type": "string",
                    "enum": ["support", "refute", "neutral"],
                }
            },
            "required": ["label"],
            "additionalProperties": False,
        }
        if self.output_schema != expected_schema:
            raise ValidationError("hosted verifier output schema drifted")

    @property
    def prompt_hash(self) -> str:
        return stable_m4_digest(
            "groundloop-hosted-verifier-prompt-v1",
            self.prompt_version,
            self.instructions,
        )

    @property
    def schema_hash(self) -> str:
        return stable_m4_digest(
            "groundloop-hosted-verifier-output-schema-v1",
            _canonical_json(self.output_schema),
        )


def load_hosted_verifier_config(path: str | Path) -> HostedVerifierConfig:
    raw_bytes = Path(path).read_bytes()
    try:
        value = json.loads(raw_bytes)
    except json.JSONDecodeError as error:
        raise ValidationError("hosted verifier config is invalid JSON") from error
    if not isinstance(value, dict):
        raise ValidationError("hosted verifier config must be an object")
    raw = cast(dict[str, Any], value)
    expected = {
        "schema_version",
        "provider",
        "model",
        "endpoint",
        "reasoning_effort",
        "max_output_tokens",
        "standard_input_usd_per_million",
        "standard_output_usd_per_million",
        "batch_discount_numerator",
        "batch_discount_denominator",
        "maximum_program_spend_usd",
        "smoke_request_limit",
        "smoke_spend_limit_usd",
        "prompt_version",
        "instructions",
        "output_schema",
    }
    if set(raw) != expected:
        raise ValidationError("hosted verifier config fields drifted")
    if raw["schema_version"] != CONFIG_SCHEMA_VERSION:
        raise ValidationError("unsupported hosted verifier config schema")
    schema = raw["output_schema"]
    if not isinstance(schema, dict):
        raise ValidationError("output_schema must be an object")
    return HostedVerifierConfig(
        provider=str(raw["provider"]),
        model=str(raw["model"]),
        endpoint=str(raw["endpoint"]),
        reasoning_effort=str(raw["reasoning_effort"]),
        max_output_tokens=_positive_integer(
            raw["max_output_tokens"], "max_output_tokens"
        ),
        standard_input_usd_per_million=_decimal(
            raw["standard_input_usd_per_million"],
            "standard_input_usd_per_million",
        ),
        standard_output_usd_per_million=_decimal(
            raw["standard_output_usd_per_million"],
            "standard_output_usd_per_million",
        ),
        batch_discount_numerator=_positive_integer(
            raw["batch_discount_numerator"], "batch_discount_numerator"
        ),
        batch_discount_denominator=_positive_integer(
            raw["batch_discount_denominator"], "batch_discount_denominator"
        ),
        maximum_program_spend_usd=_decimal(
            raw["maximum_program_spend_usd"], "maximum_program_spend_usd"
        ),
        smoke_request_limit=_positive_integer(
            raw["smoke_request_limit"], "smoke_request_limit"
        ),
        smoke_spend_limit_usd=_decimal(
            raw["smoke_spend_limit_usd"], "smoke_spend_limit_usd"
        ),
        prompt_version=str(raw["prompt_version"]),
        instructions=str(raw["instructions"]),
        output_schema=cast(dict[str, object], schema),
        config_sha256=_sha256_bytes(raw_bytes),
    )


@dataclass(frozen=True, slots=True)
class HostedVerifierRequest:
    request_id: str
    event_index: int
    judged_side: str
    claim_sha256: str
    current_evidence_sha256: str
    previous_evidence_sha256: str | None
    selected_by_task5a: bool
    estimated_input_tokens: int
    request_body: dict[str, object]

    def __post_init__(self) -> None:
        _require_sha256("request_id", self.request_id)
        _require_sha256("claim_sha256", self.claim_sha256)
        _require_sha256("current_evidence_sha256", self.current_evidence_sha256)
        if self.previous_evidence_sha256 is not None:
            _require_sha256(
                "previous_evidence_sha256", self.previous_evidence_sha256
            )
        if self.event_index < 0 or self.judged_side not in {"old", "new"}:
            raise ValidationError("invalid hosted request event or side")
        if self.estimated_input_tokens <= 0:
            raise ValidationError("estimated input tokens must be positive")

    def batch_line(self) -> str:
        return _canonical_json(
            {
                "custom_id": self.request_id,
                "method": "POST",
                "url": "/v1/responses",
                "body": self.request_body,
            }
        )

    def mapping_payload(self) -> dict[str, object]:
        payload = asdict(self)
        del payload["request_body"]
        return payload


def _request_text(
    *, claim_text: str, previous_evidence: str | None, current_evidence: str
) -> str:
    previous = (
        "[no earlier revision supplied]"
        if previous_evidence is None
        else previous_evidence
    )
    return (
        "CLAIM:\n"
        + claim_text
        + "\n\nOLD EVIDENCE:\n"
        + previous
        + "\n\nNEW EVIDENCE:\n"
        + current_evidence
    )


def _estimate_tokens(config: HostedVerifierConfig, input_text: str) -> int:
    schema_text = _canonical_json(config.output_schema)
    character_count = len(config.instructions) + len(input_text) + len(schema_text)
    return max(1, (character_count + 3) // 4)


def build_hosted_request(
    *,
    config: HostedVerifierConfig,
    event_index: int,
    judged_side: str,
    claim_sha256: str,
    claim_text: str,
    previous_evidence: str | None,
    current_evidence: str,
    selected_by_task5a: bool,
) -> HostedVerifierRequest:
    if _sha256_text(claim_text) != claim_sha256:
        raise ValidationError("hosted request claim hash mismatch")
    if judged_side == "old" and previous_evidence is not None:
        raise ValidationError("old-side request cannot receive earlier evidence")
    if judged_side == "new" and previous_evidence is None:
        raise ValidationError("new-side request requires previous evidence")
    current_hash = _sha256_text(current_evidence)
    previous_hash = (
        None if previous_evidence is None else _sha256_text(previous_evidence)
    )
    input_text = _request_text(
        claim_text=claim_text,
        previous_evidence=previous_evidence,
        current_evidence=current_evidence,
    )
    request_id = stable_m4_digest(
        REQUEST_SCHEMA_VERSION,
        config.provider,
        config.model,
        config.prompt_hash,
        config.schema_hash,
        claim_sha256,
        "none" if previous_hash is None else previous_hash,
        current_hash,
        judged_side,
    )
    body: dict[str, object] = {
        "model": config.model,
        "instructions": config.instructions,
        "input": input_text,
        "reasoning": {"effort": config.reasoning_effort},
        "max_output_tokens": config.max_output_tokens,
        "text": {
            "format": {
                "type": "json_schema",
                "name": "groundloop_grounding_judgment",
                "strict": True,
                "schema": config.output_schema,
            }
        },
        "store": False,
    }
    return HostedVerifierRequest(
        request_id=request_id,
        event_index=event_index,
        judged_side=judged_side,
        claim_sha256=claim_sha256,
        current_evidence_sha256=current_hash,
        previous_evidence_sha256=previous_hash,
        selected_by_task5a=selected_by_task5a,
        estimated_input_tokens=_estimate_tokens(config, input_text),
        request_body=body,
    )


def build_request_population(
    *, config: HostedVerifierConfig, partition: Partition
) -> tuple[HostedVerifierRequest, ...]:
    requests: list[HostedVerifierRequest] = []
    for event_index, case in enumerate(partition.cases):
        from groundloop.fyp_impact_selection import rank_claims

        selected = {
            claim.claim_sha256
            for claim in rank_claims(
                policy_id="old_new_rarity_coverage",
                claims=partition.registry,
                old_evidence=case.old_evidence,
                new_evidence=case.new_evidence,
            )[:8]
        }
        for claim in partition.registry:
            requests.append(
                build_hosted_request(
                    config=config,
                    event_index=event_index,
                    judged_side="old",
                    claim_sha256=claim.claim_sha256,
                    claim_text=claim.text,
                    previous_evidence=None,
                    current_evidence=case.old_evidence,
                    selected_by_task5a=False,
                )
            )
            requests.append(
                build_hosted_request(
                    config=config,
                    event_index=event_index,
                    judged_side="new",
                    claim_sha256=claim.claim_sha256,
                    claim_text=claim.text,
                    previous_evidence=case.old_evidence,
                    current_evidence=case.new_evidence,
                    selected_by_task5a=claim.claim_sha256 in selected,
                )
            )
    ordered = tuple(
        sorted(
            requests,
            key=lambda item: (item.event_index, item.judged_side, item.claim_sha256),
        )
    )
    identities = tuple(item.request_id for item in ordered)
    if len(identities) != len(set(identities)):
        raise ValidationError("hosted request population contains duplicate identities")
    return ordered


@dataclass(frozen=True, slots=True)
class RequestBatchPart:
    part_index: int
    request_count: int
    estimated_input_tokens: int
    estimated_max_output_tokens: int
    request_ids_hash: str


@dataclass(frozen=True, slots=True)
class HostedRequestManifest:
    schema_version: str
    config_sha256: str
    prompt_hash: str
    schema_hash: str
    request_count: int
    old_request_count: int
    new_request_count: int
    selected_new_request_count: int
    estimated_input_tokens: int
    estimated_max_output_tokens: int
    estimated_standard_cost_usd: str
    estimated_batch_cost_usd: str
    maximum_program_spend_usd: str
    request_population_hash: str
    batch_parts: tuple[RequestBatchPart, ...]

    @property
    def manifest_hash(self) -> str:
        return stable_m4_digest(
            MANIFEST_SCHEMA_VERSION,
            _canonical_json(asdict(self)),
        )

    def to_canonical_json(self) -> str:
        payload = asdict(self)
        payload["manifest_hash"] = self.manifest_hash
        return _canonical_json(payload) + "\n"


def _usd_text(value: Decimal) -> str:
    return str(value.quantize(Decimal("0.000001"), rounding=ROUND_CEILING))


def estimated_cost(
    *,
    config: HostedVerifierConfig,
    input_tokens: int,
    output_tokens: int,
    batch: bool,
) -> Decimal:
    cost = (
        Decimal(input_tokens)
        * config.standard_input_usd_per_million
        / Decimal(1_000_000)
        + Decimal(output_tokens)
        * config.standard_output_usd_per_million
        / Decimal(1_000_000)
    )
    if batch:
        cost *= Decimal(config.batch_discount_numerator) / Decimal(
            config.batch_discount_denominator
        )
    return cost


def build_request_manifest(
    *,
    config: HostedVerifierConfig,
    requests: tuple[HostedVerifierRequest, ...],
    maximum_part_requests: int = 45_000,
    maximum_part_estimated_input_tokens: int = 4_500_000,
) -> HostedRequestManifest:
    if not requests:
        raise ValidationError("hosted request manifest requires requests")
    parts: list[RequestBatchPart] = []
    current: list[HostedVerifierRequest] = []
    current_tokens = 0

    def flush() -> None:
        nonlocal current, current_tokens
        if not current:
            return
        parts.append(
            RequestBatchPart(
                part_index=len(parts),
                request_count=len(current),
                estimated_input_tokens=current_tokens,
                estimated_max_output_tokens=(
                    len(current) * config.max_output_tokens
                ),
                request_ids_hash=stable_m4_digest(
                    "groundloop-hosted-request-part-v1",
                    *(item.request_id for item in current),
                ),
            )
        )
        current = []
        current_tokens = 0

    for request in requests:
        would_exceed = current and (
            len(current) + 1 > maximum_part_requests
            or current_tokens + request.estimated_input_tokens
            > maximum_part_estimated_input_tokens
        )
        if would_exceed:
            flush()
        current.append(request)
        current_tokens += request.estimated_input_tokens
    flush()
    input_tokens = sum(item.estimated_input_tokens for item in requests)
    output_tokens = len(requests) * config.max_output_tokens
    standard = estimated_cost(
        config=config,
        input_tokens=input_tokens,
        output_tokens=output_tokens,
        batch=False,
    )
    batch_cost = estimated_cost(
        config=config,
        input_tokens=input_tokens,
        output_tokens=output_tokens,
        batch=True,
    )
    if batch_cost > config.maximum_program_spend_usd:
        raise ValidationError("projected Batch cost exceeds the program cap")
    return HostedRequestManifest(
        schema_version=MANIFEST_SCHEMA_VERSION,
        config_sha256=config.config_sha256,
        prompt_hash=config.prompt_hash,
        schema_hash=config.schema_hash,
        request_count=len(requests),
        old_request_count=sum(item.judged_side == "old" for item in requests),
        new_request_count=sum(item.judged_side == "new" for item in requests),
        selected_new_request_count=sum(
            item.judged_side == "new" and item.selected_by_task5a
            for item in requests
        ),
        estimated_input_tokens=input_tokens,
        estimated_max_output_tokens=output_tokens,
        estimated_standard_cost_usd=_usd_text(standard),
        estimated_batch_cost_usd=_usd_text(batch_cost),
        maximum_program_spend_usd=_usd_text(config.maximum_program_spend_usd),
        request_population_hash=stable_m4_digest(
            "groundloop-hosted-request-population-v1",
            *(item.request_id for item in requests),
        ),
        batch_parts=tuple(parts),
    )


def write_request_population(
    *,
    config: HostedVerifierConfig,
    requests: tuple[HostedVerifierRequest, ...],
    manifest: HostedRequestManifest,
    output_directory: str | Path,
) -> tuple[Path, ...]:
    output = Path(output_directory)
    output.mkdir(parents=True, exist_ok=True)
    manifest_path = output / "request_manifest.json"
    mapping_path = output / "request_mapping.jsonl"
    selected_path = output / "selected_new_requests.jsonl"
    manifest_path.write_text(manifest.to_canonical_json(), encoding="utf-8")
    mapping_path.write_text(
        "".join(_canonical_json(item.mapping_payload()) + "\n" for item in requests),
        encoding="utf-8",
    )
    selected_path.write_text(
        "".join(
            item.batch_line() + "\n"
            for item in requests
            if item.judged_side == "new" and item.selected_by_task5a
        ),
        encoding="utf-8",
    )
    part_paths: list[Path] = []
    offset = 0
    for part in manifest.batch_parts:
        values = requests[offset : offset + part.request_count]
        path = output / f"batch_part_{part.part_index:02d}.jsonl"
        path.write_text(
            "".join(item.batch_line() + "\n" for item in values),
            encoding="utf-8",
        )
        part_paths.append(path)
        offset += part.request_count
    if offset != len(requests):
        raise ValidationError("batch part writing omitted requests")
    return (manifest_path, mapping_path, selected_path, *part_paths)
