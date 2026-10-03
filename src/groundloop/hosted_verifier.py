"""Provider-neutral request identities and OpenAI Batch request planning.

This module builds request bytes and cost bounds. Network execution is a
separate explicit stage so ordinary tests and offline reports cannot spend.
"""

from __future__ import annotations

import hashlib
import json
import time
from dataclasses import asdict, dataclass
from decimal import ROUND_CEILING, Decimal, InvalidOperation
from pathlib import Path
from typing import Any, cast
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from groundloop.errors import ValidationError
from groundloop.fyp_impact_selection import Partition
from groundloop.m4.contracts import stable_m4_digest

CONFIG_SCHEMA_VERSION = "groundloop-hosted-verifier-config-v1"
REQUEST_SCHEMA_VERSION = "groundloop-hosted-verifier-request-v1"
MANIFEST_SCHEMA_VERSION = "groundloop-hosted-verifier-request-manifest-v1"
RESULT_SCHEMA_VERSION = "groundloop-hosted-verifier-result-v1"
BATCH_LEDGER_SCHEMA_VERSION = "groundloop-hosted-verifier-batch-ledger-v1"
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


def select_development_smoke_requests(
    *,
    requests: tuple[HostedVerifierRequest, ...],
    partition: Partition,
    limit: int,
) -> tuple[HostedVerifierRequest, ...]:
    """Choose balanced own-page old/new pairs from development only."""
    if limit <= 0 or limit % 4:
        raise ValidationError("smoke limit must be a positive multiple of four")
    strata = tuple(sorted({case.stratum for case in partition.cases}))
    if not strata or limit % len(strata):
        raise ValidationError("smoke limit cannot be balanced across strata")
    request_by_key = {
        (item.event_index, item.judged_side, item.claim_sha256): item
        for item in requests
    }
    case_count = limit // 4
    cases_per_stratum = case_count // len(strata)
    if cases_per_stratum * len(strata) != case_count:
        raise ValidationError("smoke cases cannot be balanced across strata")
    chosen_cases: list[tuple[int, object]] = []
    for stratum in strata:
        members = [
            (index, case)
            for index, case in enumerate(partition.cases)
            if case.stratum == stratum
        ]
        chosen_cases.extend(members[:cases_per_stratum])
    chosen: list[HostedVerifierRequest] = []
    for event_index, untyped_case in sorted(chosen_cases):
        from groundloop.fyp_impact_selection import RevisionCase

        case = cast(RevisionCase, untyped_case)
        for claim in case.affected_claims:
            for side in ("old", "new"):
                chosen.append(request_by_key[(event_index, side, claim.claim_sha256)])
    ordered = tuple(
        sorted(
            chosen,
            key=lambda item: (
                item.event_index,
                item.claim_sha256,
                item.judged_side,
            ),
        )
    )
    if len(ordered) != limit or len({item.request_id for item in ordered}) != limit:
        raise ValidationError("smoke request population is incomplete")
    return ordered


@dataclass(frozen=True, slots=True)
class HostedVerifierResult:
    schema_version: str
    request_id: str
    provider_response_id: str
    model: str
    label: str
    input_tokens: int
    output_tokens: int
    total_tokens: int
    elapsed_ms: int | None
    raw_response_sha256: str

    def __post_init__(self) -> None:
        if self.schema_version != RESULT_SCHEMA_VERSION:
            raise ValidationError("unsupported hosted result schema")
        _require_sha256("request_id", self.request_id)
        _require_sha256("raw_response_sha256", self.raw_response_sha256)
        if not self.provider_response_id or not self.model:
            raise ValidationError("hosted result provider identity is missing")
        if self.label not in {"support", "refute", "neutral"}:
            raise ValidationError("hosted result label is outside the schema")
        if min(self.input_tokens, self.output_tokens, self.total_tokens) < 0:
            raise ValidationError("hosted result usage cannot be negative")
        if self.total_tokens != self.input_tokens + self.output_tokens:
            raise ValidationError("hosted result total token count is inconsistent")
        if self.elapsed_ms is not None and self.elapsed_ms < 0:
            raise ValidationError("hosted result elapsed time cannot be negative")

    def to_canonical_json(self) -> str:
        return _canonical_json(asdict(self)) + "\n"


def _required_dict(value: object, name: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ValidationError(f"{name} must be an object")
    return cast(dict[str, Any], value)


def _required_int(value: object, name: str) -> int:
    if type(value) is not int or value < 0:
        raise ValidationError(f"{name} must be a nonnegative integer")
    return value


def parse_openai_response(
    *,
    request: HostedVerifierRequest,
    payload: object,
    raw_response_bytes: bytes,
    elapsed_ms: int | None,
) -> HostedVerifierResult:
    body = _required_dict(payload, "OpenAI response")
    if body.get("status") != "completed":
        raise ValidationError("OpenAI response is not completed")
    provider_id = body.get("id")
    model = body.get("model")
    if not isinstance(provider_id, str) or not isinstance(model, str):
        raise ValidationError("OpenAI response identity is invalid")
    output = body.get("output")
    if not isinstance(output, list):
        raise ValidationError("OpenAI response output must be an array")
    texts: list[str] = []
    for item_value in output:
        item = _required_dict(item_value, "OpenAI output item")
        content = item.get("content", [])
        if not isinstance(content, list):
            raise ValidationError("OpenAI output content must be an array")
        for content_value in content:
            content_item = _required_dict(content_value, "OpenAI content item")
            if content_item.get("type") == "output_text":
                text_value = content_item.get("text")
                if not isinstance(text_value, str):
                    raise ValidationError("OpenAI output text must be a string")
                texts.append(text_value)
    if len(texts) != 1:
        raise ValidationError("OpenAI response must contain one output text")
    try:
        decoded = json.loads(texts[0])
    except json.JSONDecodeError as error:
        raise ValidationError("OpenAI structured output is invalid JSON") from error
    label_payload = _required_dict(decoded, "OpenAI structured output")
    if set(label_payload) != {"label"}:
        raise ValidationError("OpenAI structured output fields drifted")
    label = label_payload["label"]
    if not isinstance(label, str):
        raise ValidationError("OpenAI structured label must be a string")
    usage = _required_dict(body.get("usage"), "OpenAI usage")
    input_tokens = _required_int(usage.get("input_tokens"), "input_tokens")
    output_tokens = _required_int(usage.get("output_tokens"), "output_tokens")
    total_tokens = _required_int(usage.get("total_tokens"), "total_tokens")
    return HostedVerifierResult(
        schema_version=RESULT_SCHEMA_VERSION,
        request_id=request.request_id,
        provider_response_id=provider_id,
        model=model,
        label=label,
        input_tokens=input_tokens,
        output_tokens=output_tokens,
        total_tokens=total_tokens,
        elapsed_ms=elapsed_ms,
        raw_response_sha256=_sha256_bytes(raw_response_bytes),
    )


@dataclass(slots=True)
class OpenAIHTTPClient:
    api_key: str
    base_url: str = "https://api.openai.com"
    timeout_seconds: float = 120.0

    def __post_init__(self) -> None:
        if not self.api_key.strip():
            raise ValidationError("OpenAI API key is empty")
        if not self.base_url.startswith("https://"):
            raise ValidationError("OpenAI base URL must use HTTPS")
        if self.timeout_seconds <= 0:
            raise ValidationError("OpenAI timeout must be positive")

    def _send(self, request: Request) -> bytes:
        try:
            with urlopen(request, timeout=self.timeout_seconds) as response:
                return cast(bytes, response.read())
        except HTTPError as error:
            detail = error.read().decode("utf-8", errors="replace")[:1000]
            raise ValidationError(
                f"OpenAI HTTP {error.code}: {detail}"
            ) from error
        except URLError as error:
            raise ValidationError(f"OpenAI network error: {error.reason}") from error

    def json_request(
        self, *, method: str, path: str, payload: object | None = None
    ) -> tuple[dict[str, Any], bytes]:
        data = None if payload is None else _canonical_json(payload).encode("utf-8")
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "User-Agent": "groundloop-fyp/0.1",
        }
        if data is not None:
            headers["Content-Type"] = "application/json"
        raw = self._send(
            Request(
                self.base_url + path,
                data=data,
                headers=headers,
                method=method,
            )
        )
        try:
            decoded = json.loads(raw)
        except json.JSONDecodeError as error:
            raise ValidationError("OpenAI returned invalid JSON") from error
        return _required_dict(decoded, "OpenAI response"), raw

    def upload_batch_file(self, path: str | Path) -> dict[str, Any]:
        file_path = Path(path)
        file_bytes = file_path.read_bytes()
        boundary = "groundloop-" + _sha256_bytes(file_bytes)[:32]
        delimiter = f"--{boundary}".encode()
        body = b"\r\n".join(
            (
                delimiter,
                b'Content-Disposition: form-data; name="purpose"',
                b"",
                b"batch",
                delimiter,
                (
                    'Content-Disposition: form-data; name="file"; filename="'
                    + file_path.name
                    + '"'
                ).encode(),
                b"Content-Type: application/jsonl",
                b"",
                file_bytes,
                delimiter + b"--",
                b"",
            )
        )
        raw = self._send(
            Request(
                self.base_url + "/v1/files",
                data=body,
                headers={
                    "Authorization": f"Bearer {self.api_key}",
                    "Content-Type": f"multipart/form-data; boundary={boundary}",
                    "User-Agent": "groundloop-fyp/0.1",
                },
                method="POST",
            )
        )
        try:
            return _required_dict(json.loads(raw), "OpenAI file response")
        except json.JSONDecodeError as error:
            raise ValidationError("OpenAI file response is invalid JSON") from error

    def download_file(self, file_id: str) -> bytes:
        return self._send(
            Request(
                self.base_url + f"/v1/files/{file_id}/content",
                headers={
                    "Authorization": f"Bearer {self.api_key}",
                    "User-Agent": "groundloop-fyp/0.1",
                },
                method="GET",
            )
        )


def run_sync_request(
    *,
    config: HostedVerifierConfig,
    request: HostedVerifierRequest,
    client: OpenAIHTTPClient,
) -> HostedVerifierResult:
    started = time.monotonic_ns()
    payload, raw = client.json_request(
        method="POST", path=config.endpoint, payload=request.request_body
    )
    elapsed_ms = (time.monotonic_ns() - started + 999_999) // 1_000_000
    return parse_openai_response(
        request=request,
        payload=payload,
        raw_response_bytes=raw,
        elapsed_ms=elapsed_ms,
    )


def run_sync_smoke(
    *,
    config: HostedVerifierConfig,
    requests: tuple[HostedVerifierRequest, ...],
    client: OpenAIHTTPClient,
) -> tuple[HostedVerifierResult, ...]:
    if len(requests) > config.smoke_request_limit:
        raise ValidationError("smoke request count exceeds the frozen limit")
    estimate = estimated_cost(
        config=config,
        input_tokens=sum(item.estimated_input_tokens for item in requests),
        output_tokens=len(requests) * config.max_output_tokens,
        batch=False,
    )
    if estimate > config.smoke_spend_limit_usd:
        raise ValidationError("smoke projected cost exceeds the frozen cap")
    results = tuple(
        run_sync_request(config=config, request=request, client=client)
        for request in requests
    )
    if len({item.request_id for item in results}) != len(requests):
        raise ValidationError("smoke results contain duplicate identities")
    return results


def write_hosted_results(
    *, results: tuple[HostedVerifierResult, ...], output_path: str | Path
) -> Path:
    if not results:
        raise ValidationError("hosted result output cannot be empty")
    ordered = tuple(sorted(results, key=lambda item: item.request_id))
    if len({item.request_id for item in ordered}) != len(ordered):
        raise ValidationError("hosted result output has duplicate request IDs")
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "".join(item.to_canonical_json() for item in ordered), encoding="utf-8"
    )
    return path


def actual_result_cost(
    *,
    config: HostedVerifierConfig,
    results: tuple[HostedVerifierResult, ...],
    batch: bool,
) -> Decimal:
    return estimated_cost(
        config=config,
        input_tokens=sum(item.input_tokens for item in results),
        output_tokens=sum(item.output_tokens for item in results),
        batch=batch,
    )


@dataclass(frozen=True, slots=True)
class BatchJobState:
    role: str
    part_index: int
    input_path: str
    input_sha256: str
    input_request_count: int
    input_file_id: str | None
    batch_id: str | None
    provider_status: str
    output_file_id: str | None
    error_file_id: str | None
    created_at: int | None
    in_progress_at: int | None
    finalizing_at: int | None
    completed_at: int | None
    output_path: str | None

    def __post_init__(self) -> None:
        if self.role not in {"baseline_old", "exhaustive_new", "selected_new"}:
            raise ValidationError("batch job role is invalid")
        if self.part_index < 0 or self.input_request_count <= 0:
            raise ValidationError("batch job dimensions are invalid")
        _require_sha256("input_sha256", self.input_sha256)


@dataclass(frozen=True, slots=True)
class BatchLedger:
    schema_version: str
    request_manifest_hash: str
    jobs: tuple[BatchJobState, ...]

    def __post_init__(self) -> None:
        if self.schema_version != BATCH_LEDGER_SCHEMA_VERSION:
            raise ValidationError("unsupported batch ledger schema")
        _require_sha256("request_manifest_hash", self.request_manifest_hash)
        keys = tuple((item.role, item.part_index) for item in self.jobs)
        if len(keys) != len(set(keys)):
            raise ValidationError("batch ledger contains duplicate jobs")

    def to_canonical_json(self) -> str:
        return _canonical_json(asdict(self)) + "\n"


def _batch_lines(
    requests: tuple[HostedVerifierRequest, ...],
) -> tuple[str, ...]:
    return tuple(item.batch_line() for item in requests)


def write_role_batch_parts(
    *,
    requests: tuple[HostedVerifierRequest, ...],
    request_manifest_hash: str,
    output_directory: str | Path,
    maximum_part_requests: int = 45_000,
    maximum_part_estimated_input_tokens: int = 4_500_000,
) -> BatchLedger:
    _require_sha256("request_manifest_hash", request_manifest_hash)
    output = Path(output_directory)
    output.mkdir(parents=True, exist_ok=True)
    role_requests = {
        "selected_new": tuple(
            item
            for item in requests
            if item.judged_side == "new" and item.selected_by_task5a
        ),
        "baseline_old": tuple(item for item in requests if item.judged_side == "old"),
        "exhaustive_new": tuple(
            item for item in requests if item.judged_side == "new"
        ),
    }
    jobs: list[BatchJobState] = []
    for role, values in role_requests.items():
        chunks: list[list[HostedVerifierRequest]] = []
        current: list[HostedVerifierRequest] = []
        current_tokens = 0
        for request in values:
            if current and (
                len(current) + 1 > maximum_part_requests
                or current_tokens + request.estimated_input_tokens
                > maximum_part_estimated_input_tokens
            ):
                chunks.append(current)
                current = []
                current_tokens = 0
            current.append(request)
            current_tokens += request.estimated_input_tokens
        if current:
            chunks.append(current)
        for part_index, chunk in enumerate(chunks):
            path = output / f"{role}_part_{part_index:02d}.jsonl"
            content = "".join(line + "\n" for line in _batch_lines(tuple(chunk)))
            path.write_text(content, encoding="utf-8")
            jobs.append(
                BatchJobState(
                    role=role,
                    part_index=part_index,
                    input_path=str(path.resolve()),
                    input_sha256=_sha256_text(content),
                    input_request_count=len(chunk),
                    input_file_id=None,
                    batch_id=None,
                    provider_status="not_submitted",
                    output_file_id=None,
                    error_file_id=None,
                    created_at=None,
                    in_progress_at=None,
                    finalizing_at=None,
                    completed_at=None,
                    output_path=None,
                )
            )
    return BatchLedger(
        schema_version=BATCH_LEDGER_SCHEMA_VERSION,
        request_manifest_hash=request_manifest_hash,
        jobs=tuple(jobs),
    )


def load_batch_ledger(path: str | Path) -> BatchLedger:
    try:
        payload = json.loads(Path(path).read_bytes())
    except json.JSONDecodeError as error:
        raise ValidationError("batch ledger is invalid JSON") from error
    body = _required_dict(payload, "batch ledger")
    if set(body) != {"schema_version", "request_manifest_hash", "jobs"}:
        raise ValidationError("batch ledger fields drifted")
    jobs_value = body["jobs"]
    if not isinstance(jobs_value, list):
        raise ValidationError("batch ledger jobs must be an array")
    return BatchLedger(
        schema_version=str(body["schema_version"]),
        request_manifest_hash=str(body["request_manifest_hash"]),
        jobs=tuple(
            BatchJobState(**_required_dict(item, "batch job"))
            for item in jobs_value
        ),
    )


def write_batch_ledger(ledger: BatchLedger, path: str | Path) -> Path:
    output = Path(path)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(ledger.to_canonical_json(), encoding="utf-8")
    return output


def _optional_int(value: object, name: str) -> int | None:
    if value is None:
        return None
    return _required_int(value, name)


def _updated_batch_job(
    job: BatchJobState, batch: dict[str, Any]
) -> BatchJobState:
    batch_id = batch.get("id")
    status = batch.get("status")
    if not isinstance(batch_id, str) or not isinstance(status, str):
        raise ValidationError("OpenAI batch identity is invalid")
    output_file_id = batch.get("output_file_id")
    error_file_id = batch.get("error_file_id")
    if output_file_id is not None and not isinstance(output_file_id, str):
        raise ValidationError("OpenAI output file identity is invalid")
    if error_file_id is not None and not isinstance(error_file_id, str):
        raise ValidationError("OpenAI error file identity is invalid")
    return BatchJobState(
        **{
            **asdict(job),
            "batch_id": batch_id,
            "provider_status": status,
            "output_file_id": output_file_id,
            "error_file_id": error_file_id,
            "created_at": _optional_int(batch.get("created_at"), "created_at"),
            "in_progress_at": _optional_int(
                batch.get("in_progress_at"), "in_progress_at"
            ),
            "finalizing_at": _optional_int(
                batch.get("finalizing_at"), "finalizing_at"
            ),
            "completed_at": _optional_int(
                batch.get("completed_at"), "completed_at"
            ),
        }
    )


def advance_batch_ledger_once(
    *,
    ledger: BatchLedger,
    client: OpenAIHTTPClient,
    output_directory: str | Path,
) -> BatchLedger:
    """Advance only the first incomplete job, preventing duplicate submission."""
    jobs = list(ledger.jobs)
    active_statuses = {"validating", "in_progress", "finalizing"}
    failure_statuses = {"failed", "expired", "cancelled", "cancelling"}
    for index, job in enumerate(jobs):
        if job.provider_status == "completed" and job.output_path is not None:
            continue
        input_path = Path(job.input_path)
        if _sha256_bytes(input_path.read_bytes()) != job.input_sha256:
            raise ValidationError("batch input bytes changed after ledger freeze")
        current = job
        if current.input_file_id is None:
            file_payload = client.upload_batch_file(input_path)
            file_id = file_payload.get("id")
            if not isinstance(file_id, str):
                raise ValidationError("OpenAI file upload omitted its identity")
            current = BatchJobState(
                **{
                    **asdict(current),
                    "input_file_id": file_id,
                    "provider_status": "uploaded",
                }
            )
        if current.batch_id is None:
            batch, _ = client.json_request(
                method="POST",
                path="/v1/batches",
                payload={
                    "input_file_id": current.input_file_id,
                    "endpoint": "/v1/responses",
                    "completion_window": "24h",
                    "metadata": {
                        "groundloop_role": current.role,
                        "groundloop_part": str(current.part_index),
                        "groundloop_manifest": ledger.request_manifest_hash,
                    },
                },
            )
            current = _updated_batch_job(current, batch)
        elif current.provider_status != "completed":
            batch, _ = client.json_request(
                method="GET", path=f"/v1/batches/{current.batch_id}"
            )
            current = _updated_batch_job(current, batch)
        if current.provider_status in failure_statuses:
            raise ValidationError(
                f"OpenAI batch {current.batch_id} ended {current.provider_status}"
            )
        if current.provider_status == "completed":
            if current.output_file_id is None:
                raise ValidationError("completed OpenAI batch has no output file")
            output = Path(output_directory)
            output.mkdir(parents=True, exist_ok=True)
            output_path = output / (
                f"{current.role}_part_{current.part_index:02d}_output.jsonl"
            )
            output_path.write_bytes(client.download_file(current.output_file_id))
            current = BatchJobState(
                **{**asdict(current), "output_path": str(output_path.resolve())}
            )
        elif current.provider_status not in active_statuses | {"uploaded"}:
            raise ValidationError(
                f"unsupported OpenAI batch status {current.provider_status}"
            )
        jobs[index] = current
        break
    return BatchLedger(
        schema_version=ledger.schema_version,
        request_manifest_hash=ledger.request_manifest_hash,
        jobs=tuple(jobs),
    )


def parse_batch_output(
    *,
    output_bytes: bytes,
    requests_by_id: dict[str, HostedVerifierRequest],
) -> tuple[HostedVerifierResult, ...]:
    results: list[HostedVerifierResult] = []
    for line_number, line in enumerate(output_bytes.splitlines(), start=1):
        try:
            value = json.loads(line)
        except json.JSONDecodeError as error:
            raise ValidationError(
                f"batch output row {line_number} is invalid JSON"
            ) from error
        row = _required_dict(value, "batch output row")
        custom_id = row.get("custom_id")
        if not isinstance(custom_id, str) or custom_id not in requests_by_id:
            raise ValidationError("batch output contains an unknown request ID")
        if row.get("error") is not None:
            raise ValidationError(f"batch output request {custom_id} failed")
        response = _required_dict(row.get("response"), "batch response")
        if response.get("status_code") != 200:
            raise ValidationError("batch output contains a non-200 response")
        body = _required_dict(response.get("body"), "batch response body")
        raw_body = _canonical_json(body).encode("utf-8")
        results.append(
            parse_openai_response(
                request=requests_by_id[custom_id],
                payload=body,
                raw_response_bytes=raw_body,
                elapsed_ms=None,
            )
        )
    ordered = tuple(sorted(results, key=lambda item: item.request_id))
    if len({item.request_id for item in ordered}) != len(ordered):
        raise ValidationError("batch output contains duplicate request IDs")
    return ordered


def collect_batch_results(
    *,
    ledger: BatchLedger,
    requests: tuple[HostedVerifierRequest, ...],
) -> tuple[HostedVerifierResult, ...]:
    if any(
        item.provider_status != "completed" or item.output_path is None
        for item in ledger.jobs
    ):
        raise ValidationError("cannot collect an incomplete batch ledger")
    requests_by_id = {item.request_id: item for item in requests}
    results = tuple(
        result
        for job in ledger.jobs
        if job.role != "selected_new"
        for result in parse_batch_output(
            output_bytes=Path(cast(str, job.output_path)).read_bytes(),
            requests_by_id=requests_by_id,
        )
    )
    expected = {
        item.request_id
        for item in requests
        if item.judged_side in {"old", "new"}
    }
    actual = {item.request_id for item in results}
    if actual != expected or len(results) != len(expected):
        raise ValidationError("collected batch results are incomplete")
    return tuple(sorted(results, key=lambda item: item.request_id))


def collect_batch_results_by_role(
    *,
    ledger: BatchLedger,
    requests: tuple[HostedVerifierRequest, ...],
) -> dict[str, tuple[HostedVerifierResult, ...]]:
    if any(
        item.provider_status != "completed" or item.output_path is None
        for item in ledger.jobs
    ):
        raise ValidationError("cannot collect an incomplete batch ledger")
    requests_by_id = {item.request_id: item for item in requests}
    collected: dict[str, list[HostedVerifierResult]] = {}
    for job in ledger.jobs:
        parsed = parse_batch_output(
            output_bytes=Path(cast(str, job.output_path)).read_bytes(),
            requests_by_id=requests_by_id,
        )
        if len(parsed) != job.input_request_count:
            raise ValidationError("batch output count differs from its input")
        collected.setdefault(job.role, []).extend(parsed)
    result = {
        role: tuple(sorted(values, key=lambda item: item.request_id))
        for role, values in collected.items()
    }
    expected = {
        "baseline_old": {
            item.request_id for item in requests if item.judged_side == "old"
        },
        "exhaustive_new": {
            item.request_id for item in requests if item.judged_side == "new"
        },
        "selected_new": {
            item.request_id
            for item in requests
            if item.judged_side == "new" and item.selected_by_task5a
        },
    }
    if set(result) != set(expected):
        raise ValidationError("batch roles differ from the frozen program")
    for role, identities in expected.items():
        values = result[role]
        if len(values) != len(identities) or {
            item.request_id for item in values
        } != identities:
            raise ValidationError(f"batch role {role} results are incomplete")
    return result
