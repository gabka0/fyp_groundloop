"""Hash-checked WiCE-to-M5 controlled adapter.

Downloaded WiCE rows are read only from a caller-supplied directory.  The
adapter verifies every manifest hash before parsing, keeps evidence sets
atomic, and emits four disjoint record families rather than writing directly
to a GroundLoop repository.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import cast

from groundloop.domain import ModelStamp, SemanticObservation, SubjectKind
from groundloop.errors import ValidationError
from groundloop.m5.digests import (
    enum_field,
    hash_field,
    int_field,
    normalize_text_v1,
    normalized_text_hash_v1,
    sequence_field,
    stable_m5_digest,
    text_field,
)
from groundloop.m5.domain import (
    ConstructionKind,
    EvidenceGroupVersion,
    EvidenceRequirementVersion,
)
from groundloop.m5.evaluation.manifest import (
    WICE_ADAPTER_VERSION,
    load_source_manifest,
    source_path,
    verify_source_files,
)
from groundloop.m5.evaluation.matching import (
    maximum_matching,
    perfect_matching_count,
)
from groundloop.m5.evaluation.records import (
    AdapterReject,
    ControlledGroupProjection,
    ControlledObservationProjection,
    EvidenceUnit,
    EvidenceUnitMember,
    ExactSystemState,
    PrimaryCohortExclusion,
    PrimaryExclusionReason,
    RejectReason,
    SourceAnnotation,
    SourceClaim,
    SourceFileSpec,
    SourceLabel,
    SourceRequirement,
    SourceSemanticState,
    SplitAudit,
    WiceAdapterResult,
    WiceAuditReport,
    WiceRowKind,
    WiceSourceManifest,
    WiceSplit,
)
from groundloop.m5.events import ObserveRequirementEvent, m5_event_payload_digest

EVIDENCE_UNIT_TEXT_SCHEMA = "wice-evidence-unit-text-v1"
CONTROLLED_PROJECTION_VERSION = "controlled-annotation-projection-v1"
CONTROLLED_POLICY_VERSION = "controlled-projection-policy-v1"
CONTROLLED_SUPPORT_THRESHOLD = 0.5
CONTROLLED_REFUTE_THRESHOLD = 0.5
CONTROLLED_TIE_RULE_VERSION = "v1"
CANONICAL_REQUIREMENT_TASK = "verify_requirement_v1"
RENDER_LIMIT_KIND = "fixed-char-v1"
MAX_RENDERED_CHARACTERS = 1200

_SUBCLAIM_ID = re.compile(r"^(.+)-(\d+)$")


class _DuplicateJsonKey(ValueError):
    pass


@dataclass(frozen=True, slots=True)
class _RawRow:
    split: WiceSplit
    kind: WiceRowKind
    row_number: int
    payload: dict[str, object]
    evidence_bytes: bytes


@dataclass(frozen=True, slots=True)
class _Parent:
    split: WiceSplit
    row_number: int
    meta_id: str
    claim: str
    label: SourceLabel
    evidence: tuple[str, ...]
    evidence_bytes: bytes


@dataclass(frozen=True, slots=True)
class _Subclaim:
    split: WiceSplit
    row_number: int
    meta_id: str
    parent_meta_id: str
    source_ordinal: int
    claim: str
    label: SourceLabel
    evidence: tuple[str, ...]
    evidence_bytes: bytes
    supporting_sentences: tuple[object, ...]


def _unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise _DuplicateJsonKey(key)
        result[key] = value
    return result


def _skip_whitespace(value: str, offset: int) -> int:
    while offset < len(value) and value[offset] in " \t\r\n":
        offset += 1
    return offset


def _extract_top_level_value_bytes(line: bytes, target_key: str) -> bytes:
    """Extract one JSON value token without canonicalizing its source bytes."""

    try:
        value = line.decode("utf-8")
    except UnicodeDecodeError as error:
        raise ValueError("row is not valid UTF-8") from error
    decoder = json.JSONDecoder()
    offset = _skip_whitespace(value, 0)
    if offset >= len(value) or value[offset] != "{":
        raise ValueError("row root is not an object")
    offset += 1
    while True:
        offset = _skip_whitespace(value, offset)
        if offset < len(value) and value[offset] == "}":
            break
        try:
            key_object, offset = decoder.raw_decode(value, offset)
        except json.JSONDecodeError as error:
            raise ValueError("cannot decode top-level key") from error
        if not isinstance(key_object, str):
            raise ValueError("top-level key is not a string")
        offset = _skip_whitespace(value, offset)
        if offset >= len(value) or value[offset] != ":":
            raise ValueError("top-level key has no value separator")
        offset = _skip_whitespace(value, offset + 1)
        start = offset
        try:
            _, offset = decoder.raw_decode(value, offset)
        except json.JSONDecodeError as error:
            raise ValueError("cannot decode top-level value") from error
        if key_object == target_key:
            return value[start:offset].encode("utf-8")
        offset = _skip_whitespace(value, offset)
        if offset < len(value) and value[offset] == ",":
            offset += 1
            continue
        if offset < len(value) and value[offset] == "}":
            break
        raise ValueError("malformed top-level object")
    raise ValueError(f"missing top-level key {target_key!r}")


def _read_rows(
    source_root: Path,
    spec: SourceFileSpec,
    rejects: list[AdapterReject],
) -> tuple[_RawRow, ...]:
    rows: list[_RawRow] = []
    raw = source_path(source_root, spec.relative_path).read_bytes()
    for row_number, line in enumerate(raw.splitlines(), start=1):
        if not line.strip():
            rejects.append(
                AdapterReject(
                    split=spec.split,
                    kind=spec.kind,
                    row_number=row_number,
                    reason=RejectReason.BLANK_JSONL_ROW,
                )
            )
            continue
        try:
            decoded = json.loads(line, object_pairs_hook=_unique_object)
        except _DuplicateJsonKey as error:
            rejects.append(
                AdapterReject(
                    split=spec.split,
                    kind=spec.kind,
                    row_number=row_number,
                    reason=RejectReason.DUPLICATE_JSON_KEY,
                    detail=str(error),
                )
            )
            continue
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            rejects.append(
                AdapterReject(
                    split=spec.split,
                    kind=spec.kind,
                    row_number=row_number,
                    reason=RejectReason.INVALID_JSON,
                    detail=type(error).__name__,
                )
            )
            continue
        if not isinstance(decoded, dict) or not all(
            isinstance(key, str) for key in decoded
        ):
            rejects.append(
                AdapterReject(
                    split=spec.split,
                    kind=spec.kind,
                    row_number=row_number,
                    reason=RejectReason.NON_OBJECT_ROW,
                )
            )
            continue
        try:
            evidence_bytes = _extract_top_level_value_bytes(line, "evidence")
        except ValueError as error:
            rejects.append(
                AdapterReject(
                    split=spec.split,
                    kind=spec.kind,
                    row_number=row_number,
                    reason=(
                        RejectReason.MALFORMED_PARENT_ROW
                        if spec.kind is WiceRowKind.CLAIM
                        else RejectReason.MALFORMED_SUBCLAIM_ROW
                    ),
                    detail=str(error),
                )
            )
            continue
        rows.append(
            _RawRow(
                split=spec.split,
                kind=spec.kind,
                row_number=row_number,
                payload=cast(dict[str, object], decoded),
                evidence_bytes=evidence_bytes,
            )
        )
    return tuple(rows)


def _row_identifier(payload: dict[str, object]) -> str | None:
    meta = payload.get("meta")
    if not isinstance(meta, dict):
        return None
    value = meta.get("id")
    return value if isinstance(value, str) and value else None


def _parse_label(value: object) -> SourceLabel | None:
    if not isinstance(value, str):
        return None
    try:
        return SourceLabel(value)
    except ValueError:
        return None


def _parse_evidence(value: object) -> tuple[str, ...] | None:
    if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
        return None
    return tuple(cast(list[str], value))


def _parse_parent(row: _RawRow, rejects: list[AdapterReject]) -> _Parent | None:
    identifier = _row_identifier(row.payload)
    claim = row.payload.get("claim")
    label = _parse_label(row.payload.get("label"))
    evidence = _parse_evidence(row.payload.get("evidence"))
    if (
        identifier is None
        or not isinstance(claim, str)
        or not normalize_text_v1(claim)
        or label is None
        or evidence is None
    ):
        rejects.append(
            AdapterReject(
                split=row.split,
                kind=row.kind,
                row_number=row.row_number,
                reason=RejectReason.MALFORMED_PARENT_ROW,
                parent_meta_id=identifier,
            )
        )
        return None
    return _Parent(
        split=row.split,
        row_number=row.row_number,
        meta_id=identifier,
        claim=claim,
        label=label,
        evidence=evidence,
        evidence_bytes=row.evidence_bytes,
    )


def _parse_subclaim(row: _RawRow, rejects: list[AdapterReject]) -> _Subclaim | None:
    identifier = _row_identifier(row.payload)
    claim = row.payload.get("claim")
    label = _parse_label(row.payload.get("label"))
    evidence = _parse_evidence(row.payload.get("evidence"))
    supporting = row.payload.get("supporting_sentences")
    if (
        identifier is None
        or not isinstance(claim, str)
        or not normalize_text_v1(claim)
        or label is None
        or evidence is None
        or not isinstance(supporting, list)
    ):
        rejects.append(
            AdapterReject(
                split=row.split,
                kind=row.kind,
                row_number=row.row_number,
                reason=(
                    RejectReason.INVALID_SUPPORTING_SENTENCES
                    if not isinstance(supporting, list)
                    else RejectReason.MALFORMED_SUBCLAIM_ROW
                ),
                subclaim_meta_id=identifier,
            )
        )
        return None
    match = _SUBCLAIM_ID.fullmatch(identifier)
    if match is None:
        rejects.append(
            AdapterReject(
                split=row.split,
                kind=row.kind,
                row_number=row.row_number,
                reason=RejectReason.INVALID_SUBCLAIM_ID,
                subclaim_meta_id=identifier,
            )
        )
        return None
    return _Subclaim(
        split=row.split,
        row_number=row.row_number,
        meta_id=identifier,
        parent_meta_id=match.group(1),
        source_ordinal=int(match.group(2)),
        claim=claim,
        label=label,
        evidence=evidence,
        evidence_bytes=row.evidence_bytes,
        supporting_sentences=tuple(cast(list[object], supporting)),
    )


def _source_document_id(split: WiceSplit, parent_meta_id: str) -> str:
    return f"wice:{split.value}:{parent_meta_id}:evidence"


def _claim_id(manifest: WiceSourceManifest, parent: _Parent) -> str:
    return stable_m5_digest(
        "wice-controlled-claim-v1",
        text_field(manifest.official_commit),
        enum_field(parent.split),
        text_field(parent.meta_id),
    )


def _group_family_id(claim_id: str) -> str:
    return stable_m5_digest("wice-controlled-group-family-v1", text_field(claim_id))


def _group_version_id(group_family_id: str, manifest_hash: str) -> str:
    return stable_m5_digest(
        "wice-controlled-group-version-v1",
        text_field(group_family_id),
        hash_field(manifest_hash),
    )


def _requirement_version_id(
    group_version_id: str,
    subclaim: _Subclaim,
    ordinal: int,
    requirement_text_hash: str,
) -> str:
    return stable_m5_digest(
        "wice-controlled-requirement-v1",
        text_field(group_version_id),
        text_field(subclaim.meta_id),
        int_field(ordinal),
        hash_field(requirement_text_hash),
    )


def _build_evidence_unit(
    *,
    subclaim: _Subclaim,
    evidence_set_ordinal: int,
    membership: object,
    source_text_by_index: dict[tuple[str, int], str],
    rejects: list[AdapterReject],
) -> tuple[EvidenceUnit, tuple[EvidenceUnitMember, ...]] | None:
    def reject(reason: RejectReason, detail: str | None = None) -> AdapterReject:
        return AdapterReject(
            split=subclaim.split,
            kind=WiceRowKind.SUBCLAIM,
            row_number=subclaim.row_number,
            reason=reason,
            parent_meta_id=subclaim.parent_meta_id,
            subclaim_meta_id=subclaim.meta_id,
            evidence_set_ordinal=evidence_set_ordinal,
            detail=detail,
        )

    if not isinstance(membership, list):
        rejects.append(reject(RejectReason.MALFORMED_MEMBERSHIP))
        return None
    members_raw = cast(list[object], membership)
    if not members_raw:
        if subclaim.label is SourceLabel.SUPPORTED:
            rejects.append(reject(RejectReason.EMPTY_SUPPORTING_SET))
        return None
    if any(
        isinstance(index, bool) or not isinstance(index, int) for index in members_raw
    ):
        rejects.append(reject(RejectReason.NONINTEGER_SENTENCE_INDEX))
        return None
    indices = cast(list[int], members_raw)
    if any(index < 0 for index in indices):
        rejects.append(reject(RejectReason.NEGATIVE_SENTENCE_INDEX))
        return None
    if any(index >= len(subclaim.evidence) for index in indices):
        rejects.append(reject(RejectReason.OUT_OF_RANGE_SENTENCE_INDEX))
        return None
    if len(set(indices)) != len(indices):
        rejects.append(reject(RejectReason.DUPLICATE_SENTENCE_INDEX))
        return None

    source_id = _source_document_id(subclaim.split, subclaim.parent_meta_id)
    normalized_by_index: list[tuple[int, str]] = []
    for index in indices:
        raw_sentence = subclaim.evidence[index]
        source_key = (source_id, index)
        previous = source_text_by_index.get(source_key)
        if previous is not None and previous != raw_sentence:
            rejects.append(reject(RejectReason.CONFLICTING_SOURCE_TEXT))
            return None
        source_text_by_index[source_key] = raw_sentence
        normalized = normalize_text_v1(raw_sentence)
        if not normalized:
            rejects.append(reject(RejectReason.EMPTY_NORMALIZED_SENTENCE))
            return None
        normalized_by_index.append((index, normalized))

    ordered_members = tuple(sorted(normalized_by_index))
    member_sequences = tuple(
        sequence_field(
            (
                text_field(source_id),
                int_field(index),
                text_field(sentence),
            )
        )
        for index, sentence in ordered_members
    )
    evidence_unit_id = stable_m5_digest(
        WICE_ADAPTER_VERSION,
        int_field(len(ordered_members)),
        sequence_field(member_sequences),
    )
    content_sentences = tuple(sorted({sentence for _, sentence in ordered_members}))
    content_json = json.dumps(
        {"schema": EVIDENCE_UNIT_TEXT_SCHEMA, "sentences": content_sentences},
        sort_keys=True,
        ensure_ascii=False,
        separators=(",", ":"),
    )
    rendered_text = "\n\n".join(content_sentences)
    if len(rendered_text) > MAX_RENDERED_CHARACTERS:
        rejects.append(
            reject(
                RejectReason.EVIDENCE_UNIT_OVERLENGTH,
                detail=(
                    f"characters={len(rendered_text)} limit={MAX_RENDERED_CHARACTERS}"
                ),
            )
        )
        return None
    unit = EvidenceUnit(
        evidence_unit_id=evidence_unit_id,
        text_hash=normalized_text_hash_v1(content_json),
        content_json=content_json,
        normalized_sentences=content_sentences,
        rendered_text=rendered_text,
        rendered_sha256=hashlib.sha256(rendered_text.encode("utf-8")).hexdigest(),
        rendered_character_count=len(rendered_text),
    )
    member_records = tuple(
        EvidenceUnitMember(
            evidence_unit_id=evidence_unit_id,
            source_document_id=source_id,
            sentence_index=index,
            normalized_sentence=sentence,
        )
        for index, sentence in ordered_members
    )
    return unit, member_records


def _source_annotation(
    manifest: WiceSourceManifest,
    subclaim: _Subclaim,
    ordinal: int,
    unit: EvidenceUnit,
) -> SourceAnnotation:
    annotation_id = stable_m5_digest(
        "wice-source-annotation-v1",
        text_field(manifest.official_commit),
        text_field(subclaim.split.value),
        text_field(subclaim.parent_meta_id),
        text_field(subclaim.meta_id),
        int_field(ordinal),
        text_field(unit.evidence_unit_id),
        enum_field(subclaim.label),
    )
    return SourceAnnotation(
        source_annotation_id=annotation_id,
        split=subclaim.split,
        parent_meta_id=subclaim.parent_meta_id,
        subclaim_meta_id=subclaim.meta_id,
        evidence_set_ordinal=ordinal,
        evidence_unit_id=unit.evidence_unit_id,
        text_hash=unit.text_hash,
        source_label=subclaim.label,
    )


def _controlled_projection(
    *,
    manifest: WiceSourceManifest,
    requirement: SourceRequirement,
    annotation: SourceAnnotation,
    unit: EvidenceUnit,
) -> ControlledObservationProjection:
    if annotation.source_label is not SourceLabel.SUPPORTED:
        raise ValidationError("WiCE primary projection accepts supported labels only")
    projected_label = "support"
    input_hash = stable_m5_digest(
        "controlled-projection-input-v1",
        text_field(annotation.source_annotation_id),
        text_field(requirement.requirement_version_id),
        hash_field(requirement.requirement_text_hash),
        text_field(unit.evidence_unit_id),
        hash_field(unit.text_hash),
        hash_field(manifest.manifest_hash),
        enum_field(projected_label),
    )
    observation_id = stable_m5_digest(
        "controlled-observation-v1",
        text_field(annotation.source_annotation_id),
        text_field(requirement.requirement_version_id),
        text_field(unit.evidence_unit_id),
        text_field(CONTROLLED_PROJECTION_VERSION),
        hash_field(input_hash),
    )
    observation = SemanticObservation(
        observation_id=observation_id,
        subject_kind=SubjectKind.REQUIREMENT,
        subject_id=requirement.requirement_version_id,
        chunk_version_id=unit.evidence_unit_id,
        task_type=CANONICAL_REQUIREMENT_TASK,
        support_score=1.0,
        refute_score=0.0,
        neutral_score=0.0,
        producer=ModelStamp(
            model_id="controlled-annotation-projection",
            model_version=CONTROLLED_PROJECTION_VERSION,
            prompt_version="not-applicable-v1",
        ),
        input_hash=input_hash,
    )
    event_id = stable_m5_digest(
        "controlled-observe-requirement-event-v1", text_field(observation_id)
    )
    event = ObserveRequirementEvent(event_id=event_id, observation=observation)
    return ControlledObservationProjection(
        observation_id=observation_id,
        source_annotation_id=annotation.source_annotation_id,
        projection_version=CONTROLLED_PROJECTION_VERSION,
        split=requirement.split,
        manifest_hash=manifest.manifest_hash,
        projected_label=projected_label,
        event_id=event_id,
        event_payload_hash=m5_event_payload_digest(event),
        observation=observation,
    )


def _histogram(values: list[int]) -> tuple[tuple[int, int], ...]:
    return tuple(sorted(Counter(values).items()))


def _one_primary_exclusion(
    exclusions: list[PrimaryCohortExclusion],
    parent: _Parent,
    reason: PrimaryExclusionReason,
    subclaim_meta_id: str | None = None,
) -> None:
    exclusions.append(
        PrimaryCohortExclusion(
            split=parent.split,
            parent_meta_id=parent.meta_id,
            reason=reason,
            subclaim_meta_id=subclaim_meta_id,
        )
    )


def adapt_wice(
    source_root: Path,
    manifest: WiceSourceManifest,
) -> WiceAdapterResult:
    """Derive the frozen retrospective cohort from hash-verified JSONL files."""

    verified_files = verify_source_files(source_root, manifest)
    rejects: list[AdapterReject] = []
    raw_rows: dict[tuple[WiceSplit, WiceRowKind], tuple[_RawRow, ...]] = {}
    for spec in manifest.source_files:
        raw_rows[(spec.split, spec.kind)] = _read_rows(source_root, spec, rejects)

    parent_candidates: dict[tuple[WiceSplit, str], list[_Parent]] = defaultdict(list)
    for split in WiceSplit:
        for row in raw_rows[(split, WiceRowKind.CLAIM)]:
            parsed_parent = _parse_parent(row, rejects)
            if parsed_parent is not None:
                parent_candidates[(split, parsed_parent.meta_id)].append(parsed_parent)

    parents: dict[tuple[WiceSplit, str], _Parent] = {}
    for parent_candidate_key, candidates in sorted(parent_candidates.items()):
        if len(candidates) == 1:
            parents[parent_candidate_key] = candidates[0]
            continue
        for candidate in candidates:
            rejects.append(
                AdapterReject(
                    split=candidate.split,
                    kind=WiceRowKind.CLAIM,
                    row_number=candidate.row_number,
                    reason=RejectReason.DUPLICATE_PARENT_ID,
                    parent_meta_id=candidate.meta_id,
                )
            )

    parsed_subclaims: list[_Subclaim] = []
    for split in WiceSplit:
        for row in raw_rows[(split, WiceRowKind.SUBCLAIM)]:
            parsed_subclaim = _parse_subclaim(row, rejects)
            if parsed_subclaim is not None:
                parsed_subclaims.append(parsed_subclaim)

    duplicate_subclaim_keys = {
        key
        for key, count in Counter(
            (row.split, row.meta_id) for row in parsed_subclaims
        ).items()
        if count > 1
    }
    subclaims_by_parent: dict[tuple[WiceSplit, str], list[_Subclaim]] = defaultdict(
        list
    )
    invalid_mapping_parents: set[tuple[WiceSplit, str]] = set()
    mapped_by_split: Counter[WiceSplit] = Counter()
    for subclaim in sorted(
        parsed_subclaims,
        key=lambda row: (row.split.value, row.parent_meta_id, row.source_ordinal),
    ):
        parent_key = (subclaim.split, subclaim.parent_meta_id)
        if (subclaim.split, subclaim.meta_id) in duplicate_subclaim_keys:
            rejects.append(
                AdapterReject(
                    split=subclaim.split,
                    kind=WiceRowKind.SUBCLAIM,
                    row_number=subclaim.row_number,
                    reason=RejectReason.DUPLICATE_SUBCLAIM_ID,
                    parent_meta_id=subclaim.parent_meta_id,
                    subclaim_meta_id=subclaim.meta_id,
                )
            )
            invalid_mapping_parents.add(parent_key)
            continue
        parent = parents.get(parent_key)
        if parent is None:
            rejects.append(
                AdapterReject(
                    split=subclaim.split,
                    kind=WiceRowKind.SUBCLAIM,
                    row_number=subclaim.row_number,
                    reason=RejectReason.MISSING_SAME_SPLIT_PARENT,
                    parent_meta_id=subclaim.parent_meta_id,
                    subclaim_meta_id=subclaim.meta_id,
                )
            )
            continue
        if subclaim.evidence_bytes != parent.evidence_bytes:
            rejects.append(
                AdapterReject(
                    split=subclaim.split,
                    kind=WiceRowKind.SUBCLAIM,
                    row_number=subclaim.row_number,
                    reason=RejectReason.EVIDENCE_ARRAY_BYTE_MISMATCH,
                    parent_meta_id=subclaim.parent_meta_id,
                    subclaim_meta_id=subclaim.meta_id,
                )
            )
            invalid_mapping_parents.add(parent_key)
            continue
        subclaims_by_parent[parent_key].append(subclaim)
        mapped_by_split[subclaim.split] += 1

    evidence_units_by_id: dict[str, EvidenceUnit] = {}
    members_by_key: dict[tuple[str, str, int], EvidenceUnitMember] = {}
    annotations_by_subclaim: dict[tuple[WiceSplit, str], list[SourceAnnotation]] = (
        defaultdict(list)
    )
    all_annotations: list[SourceAnnotation] = []
    source_text_by_index: dict[tuple[str, int], str] = {}
    for subclaim in sorted(
        (item for values in subclaims_by_parent.values() for item in values),
        key=lambda row: (row.split.value, row.parent_meta_id, row.source_ordinal),
    ):
        for ordinal, membership in enumerate(subclaim.supporting_sentences):
            built = _build_evidence_unit(
                subclaim=subclaim,
                evidence_set_ordinal=ordinal,
                membership=membership,
                source_text_by_index=source_text_by_index,
                rejects=rejects,
            )
            if built is None:
                continue
            unit, members = built
            previous_unit = evidence_units_by_id.get(unit.evidence_unit_id)
            if previous_unit is not None and previous_unit != unit:
                raise ValidationError("evidence-unit digest collision")
            evidence_units_by_id[unit.evidence_unit_id] = unit
            for member in members:
                member_key = (
                    member.evidence_unit_id,
                    member.source_document_id,
                    member.sentence_index,
                )
                previous_member = members_by_key.get(member_key)
                if previous_member is not None and previous_member != member:
                    raise ValidationError("evidence-unit member collision")
                members_by_key[member_key] = member
            annotation = _source_annotation(manifest, subclaim, ordinal, unit)
            annotations_by_subclaim[(subclaim.split, subclaim.meta_id)].append(
                annotation
            )
            all_annotations.append(annotation)

    source_claims: list[SourceClaim] = []
    source_requirements: list[SourceRequirement] = []
    source_states: list[SourceSemanticState] = []
    group_projections: list[ControlledGroupProjection] = []
    observation_projections: list[ControlledObservationProjection] = []
    exact_states: list[ExactSystemState] = []
    exclusions: list[PrimaryCohortExclusion] = []
    primary_parent_ids: dict[WiceSplit, set[str]] = defaultdict(set)
    primary_candidate_subclaim_ids: dict[WiceSplit, set[str]] = defaultdict(set)
    unit_lookup = evidence_units_by_id

    for parent_key, parent in sorted(
        parents.items(), key=lambda item: (item[0][0].value, item[0][1])
    ):
        if parent.label is not SourceLabel.SUPPORTED:
            _one_primary_exclusion(
                exclusions, parent, PrimaryExclusionReason.PARENT_NOT_SUPPORTED
            )
            continue
        failed = False
        if parent_key in invalid_mapping_parents:
            _one_primary_exclusion(
                exclusions, parent, PrimaryExclusionReason.INVALID_SUBCLAIM_MAPPING
            )
            failed = True
        subclaims = tuple(
            sorted(
                subclaims_by_parent.get(parent_key, ()),
                key=lambda item: (item.source_ordinal, item.meta_id),
            )
        )
        if not subclaims:
            _one_primary_exclusion(
                exclusions, parent, PrimaryExclusionReason.NO_FINAL_SUBCLAIMS
            )
            failed = True
        elif len(subclaims) > 8:
            _one_primary_exclusion(
                exclusions, parent, PrimaryExclusionReason.TOO_MANY_FINAL_SUBCLAIMS
            )
            failed = True
        if subclaims and tuple(item.source_ordinal for item in subclaims) != tuple(
            range(len(subclaims))
        ):
            _one_primary_exclusion(
                exclusions, parent, PrimaryExclusionReason.SUBCLAIM_ORDINAL_NOT_DENSE
            )
            failed = True
        unsupported_subclaims = tuple(
            item for item in subclaims if item.label is not SourceLabel.SUPPORTED
        )
        for subclaim in unsupported_subclaims:
            _one_primary_exclusion(
                exclusions,
                parent,
                PrimaryExclusionReason.SUBCLAIM_NOT_SUPPORTED,
                subclaim.meta_id,
            )
        if unsupported_subclaims:
            failed = True
        requirement_hashes = tuple(
            normalized_text_hash_v1(item.claim) for item in subclaims
        )
        if len(requirement_hashes) != len(set(requirement_hashes)):
            _one_primary_exclusion(
                exclusions, parent, PrimaryExclusionReason.DUPLICATE_REQUIREMENT_TEXT
            )
            failed = True
        if not failed:
            primary_candidate_subclaim_ids[parent.split].update(
                item.meta_id for item in subclaims
            )
        valid_annotations_by_subclaim: dict[str, tuple[SourceAnnotation, ...]] = {}
        for subclaim in subclaims:
            valid = tuple(
                sorted(
                    (
                        annotation
                        for annotation in annotations_by_subclaim.get(
                            (subclaim.split, subclaim.meta_id), ()
                        )
                        if annotation.source_label is SourceLabel.SUPPORTED
                    ),
                    key=lambda item: (
                        item.evidence_unit_id,
                        item.evidence_set_ordinal,
                    ),
                )
            )
            valid_annotations_by_subclaim[subclaim.meta_id] = valid
            if subclaim.label is SourceLabel.SUPPORTED and not valid:
                _one_primary_exclusion(
                    exclusions,
                    parent,
                    PrimaryExclusionReason.UNREPRESENTABLE_POSITIVE_REQUIREMENT,
                    subclaim.meta_id,
                )
                failed = True
        if failed:
            continue

        claim_id = _claim_id(manifest, parent)
        family_id = _group_family_id(claim_id)
        group_id = _group_version_id(family_id, manifest.manifest_hash)
        requirement_records: list[SourceRequirement] = []
        domain_requirements: list[EvidenceRequirementVersion] = []
        for ordinal, subclaim in enumerate(subclaims):
            normalized_requirement = normalize_text_v1(subclaim.claim)
            requirement_hash = normalized_text_hash_v1(normalized_requirement)
            requirement_id = _requirement_version_id(
                group_id, subclaim, ordinal, requirement_hash
            )
            requirement = SourceRequirement(
                requirement_version_id=requirement_id,
                group_version_id=group_id,
                split=parent.split,
                parent_meta_id=parent.meta_id,
                subclaim_meta_id=subclaim.meta_id,
                ordinal=ordinal,
                source_requirement_text=subclaim.claim,
                normalized_requirement_text=normalized_requirement,
                requirement_text_hash=requirement_hash,
                source_label=subclaim.label,
            )
            requirement_records.append(requirement)
            domain_requirements.append(
                EvidenceRequirementVersion(
                    requirement_version_id=requirement_id,
                    group_version_id=group_id,
                    ordinal=ordinal,
                    requirement_text=normalized_requirement,
                    requirement_text_hash=requirement_hash,
                )
            )
        group = EvidenceGroupVersion(
            group_version_id=group_id,
            group_family_id=family_id,
            owner_claim_id=claim_id,
            requirements=tuple(domain_requirements),
            construction_kind=ConstructionKind.CONTROLLED,
            construction_source_id=f"wice:{manifest.official_commit}",
        )
        source_claims.append(
            SourceClaim(
                claim_id=claim_id,
                split=parent.split,
                parent_meta_id=parent.meta_id,
                source_claim_text=parent.claim,
                source_label=parent.label,
                source_document_id=_source_document_id(parent.split, parent.meta_id),
                group_family_id=family_id,
                group_version_id=group_id,
            )
        )
        source_requirements.extend(requirement_records)
        group_projections.append(
            ControlledGroupProjection(
                split=parent.split,
                parent_meta_id=parent.meta_id,
                manifest_hash=manifest.manifest_hash,
                group=group,
            )
        )
        source_states.append(
            SourceSemanticState(
                claim_id=claim_id,
                split=parent.split,
                parent_meta_id=parent.meta_id,
                direct_source_support=False,
                requirement_source_conjunction=True,
                source_semantic_label=True,
            )
        )
        edges_by_ordinal = tuple(
            tuple(
                sorted(
                    {
                        annotation.text_hash
                        for annotation in valid_annotations_by_subclaim[
                            requirement.subclaim_meta_id
                        ]
                    }
                )
            )
            for requirement in requirement_records
        )
        matching_size, assignment = maximum_matching(edges_by_ordinal)
        matching_count = perfect_matching_count(edges_by_ordinal)
        exact_states.append(
            ExactSystemState(
                claim_id=claim_id,
                group_version_id=group_id,
                requirement_count=len(requirement_records),
                distinct_content_count=len(
                    {value for edges in edges_by_ordinal for value in edges}
                ),
                matching_size=matching_size,
                groundloop_sdr_complete=matching_size == len(requirement_records),
                assignment_by_ordinal=assignment,
                perfect_matching_count=matching_count,
            )
        )
        primary_parent_ids[parent.split].add(parent.meta_id)

        requirement_by_subclaim = {
            item.subclaim_meta_id: item for item in requirement_records
        }
        projection_candidates = [
            annotation
            for requirement in requirement_records
            for annotation in valid_annotations_by_subclaim[
                requirement.subclaim_meta_id
            ]
        ]
        least_by_key: dict[tuple[str, str, str, str, str], SourceAnnotation] = {}
        for annotation in sorted(
            projection_candidates,
            key=lambda item: (
                item.split.value,
                item.parent_meta_id,
                item.subclaim_meta_id,
                item.evidence_unit_id,
                item.source_label.value,
                item.evidence_set_ordinal,
            ),
        ):
            projection_key = (
                annotation.split.value,
                annotation.parent_meta_id,
                annotation.subclaim_meta_id,
                annotation.evidence_unit_id,
                annotation.source_label.value,
            )
            least_by_key.setdefault(projection_key, annotation)
        for projection_key in sorted(least_by_key):
            annotation = least_by_key[projection_key]
            observation_projections.append(
                _controlled_projection(
                    manifest=manifest,
                    requirement=requirement_by_subclaim[annotation.subclaim_meta_id],
                    annotation=annotation,
                    unit=unit_lookup[annotation.evidence_unit_id],
                )
            )

    split_audits: list[SplitAudit] = []
    source_file_by_pair = {
        (item.split, item.kind): item for item in manifest.source_files
    }
    exact_by_claim = {item.claim_id: item for item in exact_states}
    claims_by_split: dict[WiceSplit, list[SourceClaim]] = defaultdict(list)
    for claim in source_claims:
        claims_by_split[claim.split].append(claim)
    for split in WiceSplit:
        split_claims = claims_by_split[split]
        split_states = [exact_by_claim[claim.claim_id] for claim in split_claims]
        requirement_counts = [state.requirement_count for state in split_states]
        distinct_counts = [state.distinct_content_count for state in split_states]
        matching_counts = [state.perfect_matching_count for state in split_states]
        overlapping = 0
        # Overlap is equivalent to fewer distinct hashes than the sum of
        # per-requirement degrees. Compute directly from the retained source rows.
        for claim in split_claims:
            parent_subclaims = subclaims_by_parent[(split, claim.parent_meta_id)]
            per_requirement_hashes = [
                {
                    annotation.text_hash
                    for annotation in annotations_by_subclaim[
                        (item.split, item.meta_id)
                    ]
                    if annotation.source_label is SourceLabel.SUPPORTED
                }
                for item in parent_subclaims
            ]
            degrees = Counter(
                text_hash for hashes in per_requirement_hashes for text_hash in hashes
            )
            if any(count > 1 for count in degrees.values()):
                overlapping += 1
        candidate_subclaims = primary_candidate_subclaim_ids[split]
        primary_overlength = sum(
            reject.reason is RejectReason.EVIDENCE_UNIT_OVERLENGTH
            and reject.subclaim_meta_id in candidate_subclaims
            for reject in rejects
        )
        split_audits.append(
            SplitAudit(
                split=split,
                parent_rows=source_file_by_pair[(split, WiceRowKind.CLAIM)].rows,
                subclaim_rows=source_file_by_pair[(split, WiceRowKind.SUBCLAIM)].rows,
                mapped_subclaim_rows=mapped_by_split[split],
                source_supported_parents=sum(
                    parent.label is SourceLabel.SUPPORTED
                    for (parent_split, _), parent in parents.items()
                    if parent_split is split
                ),
                representable_primary_parents=len(split_claims),
                hall_complete_parents=sum(
                    state.groundloop_sdr_complete for state in split_states
                ),
                hall_failing_parents=sum(
                    not state.groundloop_sdr_complete for state in split_states
                ),
                primary_overlength_annotations=primary_overlength,
                requirement_count_histogram=_histogram(requirement_counts),
                distinct_content_count_histogram=_histogram(distinct_counts),
                perfect_matching_count_histogram=_histogram(matching_counts),
                overlapping_parent_count=overlapping,
                alternative_assignment_parent_count=sum(
                    count > 1 for count in matching_counts
                ),
            )
        )

    # The adapter has no claim-subject path. This is asserted before reporting,
    # rather than inferred later from model IDs or scores.
    if any(
        projection.observation.subject_kind is not SubjectKind.REQUIREMENT
        for projection in observation_projections
    ):
        raise AssertionError("WiCE controlled projections must be REQUIREMENT-only")
    rejection_counts = tuple(
        sorted(
            (reason.value, count)
            for reason, count in Counter(reject.reason for reject in rejects).items()
        )
    )
    exclusion_counts = tuple(
        sorted(
            (reason.value, count)
            for reason, count in Counter(
                exclusion.reason for exclusion in exclusions
            ).items()
        )
    )
    audit = WiceAuditReport(
        schema="groundloop-wice-audit-v1",
        adapter_version=manifest.adapter_version,
        source_revision=manifest.official_commit,
        manifest_hash=manifest.manifest_hash,
        verified_files=verified_files,
        split_audits=tuple(split_audits),
        rejection_counts=rejection_counts,
        primary_exclusion_counts=exclusion_counts,
        direct_claim_projection_count=0,
        model_call_count=0,
        test_selection_performed=False,
    )
    return WiceAdapterResult(
        manifest=manifest,
        source_claims=tuple(source_claims),
        source_requirements=tuple(source_requirements),
        evidence_units=tuple(
            evidence_units_by_id[key] for key in sorted(evidence_units_by_id)
        ),
        evidence_unit_members=tuple(
            members_by_key[key] for key in sorted(members_by_key)
        ),
        source_annotations=tuple(
            sorted(
                all_annotations,
                key=lambda item: (
                    item.split.value,
                    item.parent_meta_id,
                    item.subclaim_meta_id,
                    item.evidence_set_ordinal,
                    item.evidence_unit_id,
                ),
            )
        ),
        source_semantic_states=tuple(source_states),
        controlled_groups=tuple(group_projections),
        controlled_projections=tuple(observation_projections),
        exact_system_states=tuple(exact_states),
        frozen_model_diagnostics=(),
        rejects=tuple(
            sorted(
                rejects,
                key=lambda item: (
                    item.split.value,
                    item.kind.value,
                    item.row_number,
                    item.evidence_set_ordinal
                    if item.evidence_set_ordinal is not None
                    else -1,
                    item.reason.value,
                ),
            )
        ),
        primary_exclusions=tuple(
            sorted(
                exclusions,
                key=lambda item: (
                    item.split.value,
                    item.parent_meta_id,
                    item.reason.value,
                    item.subclaim_meta_id or "",
                ),
            )
        ),
        audit=audit,
    )


def load_and_adapt_wice(source_root: Path, manifest_path: Path) -> WiceAdapterResult:
    """Convenience entry point used by the CLI and reproduction tests."""

    return adapt_wice(source_root, load_source_manifest(manifest_path))


__all__ = [
    "CANONICAL_REQUIREMENT_TASK",
    "CONTROLLED_POLICY_VERSION",
    "CONTROLLED_PROJECTION_VERSION",
    "CONTROLLED_REFUTE_THRESHOLD",
    "CONTROLLED_SUPPORT_THRESHOLD",
    "CONTROLLED_TIE_RULE_VERSION",
    "EVIDENCE_UNIT_TEXT_SCHEMA",
    "MAX_RENDERED_CHARACTERS",
    "RENDER_LIMIT_KIND",
    "adapt_wice",
    "load_and_adapt_wice",
]
