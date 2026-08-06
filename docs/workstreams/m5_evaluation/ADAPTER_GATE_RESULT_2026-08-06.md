# M5.5 controlled-adapter gate result

Date: 2026-08-06

Execution UTC: `2026-08-06T10:51:23Z`

Execution code/config commit: `d3dcc8e07094e014637016b736e87b263bd21030`

Lane: E1, `workstream/m5-evaluation-adapter-gate`

Verdict: **GO for the pinned WiCE adapter, source audit, rejection/exclusion
matrix, and pure evaluation-protocol evidence. NO_GO for maintained-runtime
M5.5 evidence or M5.5 closure.**

This lane changed only its two coordinator-authorized paths. It did not edit
adapter source, evaluation policy, configs, scripts, runtime, PostgreSQL,
top-level status documents, downloaded data, model artifacts, generated
reports, or user-owned work.

## 1. Pinned source and configuration identity

The adapter consumed the existing ignored local WiCE checkout at official
commit:

```text
ddeb6c183665e2a20c5f03c5aa07f03888b9870f
```

`git status --short` in that source checkout was empty. The adapter verified
every consumed byte before parsing. The exact source hashes were:

| Consumed file | SHA-256 |
|---|---|
| `data/entailment_retrieval/claim/train.jsonl` | `3ef74c7203e1d9b369cb2c145e764d8ab4743f008f9765fa47f9bdd6ffa2d2e1` |
| `data/entailment_retrieval/claim/dev.jsonl` | `67531ca79bde4c81d3752fb69d9cb0d3d6763de5c3028f548b83ef053a6f8042` |
| `data/entailment_retrieval/claim/test.jsonl` | `4c91b9e9590cfcd8f8a0f7288b5ff315af8ade946d7bf26f50fbd671bb86dbe8` |
| `data/entailment_retrieval/subclaim/train.jsonl` | `e8ba1ed589e22c5f9d41c8d858dd10bdc17665d224f8ce28be9d9024239cf513` |
| `data/entailment_retrieval/subclaim/dev.jsonl` | `4747a5a60c85c0c12cbf1cc7a2ab4bc9279fd4fd01a83d36630b7151330b6f57` |
| `data/entailment_retrieval/subclaim/test.jsonl` | `3202b7bff5979dfbca745f48edd8da5ad166da0886fd15042cc1ab9d2010ee19` |
| `LICENSE.md` | `f96619e6c5d30955769f82eb72a20f816361c2682c099e6e689e5e82a0352757` |
| `README.md` | `8359490c1b93bded804ab7760c0f8aa064a22049101d02ed69c8acb51965ea24` |

The annotations are recorded as ODC-BY. Wikipedia and Common Crawl terms
remain applicable to the underlying text. No source file is committed to
GroundLoop.

Frozen identities:

```text
controlled config canonical SHA-256
  4ae7e27b9ada58965cbb72037739006d4caa728f955c27caf0bcc93bbee7532f
source manifest canonical SHA-256
  c4a186f42252c280b5cafdc26b182e88c5aec14072866af15dd86d4b4c4722f2
classification
  checked_in_frozen_primary
```

## 2. Complete rejection and exclusion matrix

`tests/m5/evaluation/test_wice_reject_matrix.py` now proves that the public
adapter exposes every reachable `RejectReason` and every
`PrimaryExclusionReason` through deterministic byte fixtures.

The public-adapter matrix covers 20 rejection reasons:

```text
blank_jsonl_row
invalid_json
duplicate_json_key
non_object_row
malformed_parent_row
malformed_subclaim_row
duplicate_parent_id
duplicate_subclaim_id
invalid_subclaim_id
missing_same_split_parent
evidence_array_byte_mismatch
invalid_supporting_sentences
malformed_membership
empty_supporting_set
noninteger_sentence_index
negative_sentence_index
out_of_range_sentence_index
duplicate_sentence_index
empty_normalized_sentence
evidence_unit_overlength
```

The remaining `conflicting_source_text` branch is a defensive internal guard.
The public mapping requires same-split parent existence and byte-identical
parent/subclaim evidence arrays before evidence-unit construction, so a valid
public source cannot reach that branch. A focused unit test preloads the
source-index invariant with conflicting text and proves the guard emits the
frozen rejection rather than creating a unit.

All eight primary-cohort exclusions are covered:

```text
parent_not_supported
no_final_subclaims
too_many_final_subclaims
subclaim_ordinal_not_dense
invalid_subclaim_mapping
subclaim_not_supported
duplicate_requirement_text
unrepresentable_positive_requirement
```

The test checks both row-level records and the aggregate audit counters; it
does not merely enumerate enum names.

## 3. Pinned-byte adapter reproduction

The opt-in pinned-source test ran, not skipped, with:

```bash
GROUNDLOOP_WICE_SOURCE_ROOT=/home/kassym/Desktop/groundloop/models/m3/verifier-run-20260718/raw/wice_repo \
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src:tests \
/home/kassym/Desktop/groundloop/.venv/bin/python -m pytest -ra \
  tests/m5/evaluation/test_wice_reject_matrix.py -p no:cacheprovider
```

Result:

```text
3 passed in 2.59s
```

The pinned audit independently reproduced:

| Split | Representable parents | Hall complete | Hall failing | Primary overlength annotations |
|---|---:|---:|---:|---:|
| train | 460 | 362 | 98 | 0 |
| dev | 114 | 98 | 16 | 4 |
| test | 110 | 89 | 21 | 2 |
| total | 684 | 549 | 135 | 6 |

Additional exact counts:

```text
requirements                         1,796
least-ordinal REQUIREMENT projections 3,573
CLAIM projections                        0
model calls                              0
all-source overlength rejects           13
parent-not-supported exclusions      1,281
unrepresentable-positive exclusions      2
```

This confirms the Hall-failing cohort remains source-supported and retained.
It is not relabelled as negative source truth.

## 4. Deterministic pure-report reproduction

The checked-in runner was executed twice without latency measurement. Both
outputs were byte-identical:

```text
report schema  groundloop-m5-controlled-evaluation-suite-v1
bytes          694,233
SHA-256        007617dafaaec715df2a7464c8490baa6737598dca3203e9237ff499d5b3a810
```

The audit-only output was:

```text
schema bytes   16,448
SHA-256        65546daaa3bdf317fe206a2acdc15cd3d90ebab573bb4593a7632db833e49bae
```

Generated reports were written only under `/tmp` and are not Git artifacts.
The current report schema records the absolute consumed config path, so the
whole-report byte hash above binds this exact worktree path. The canonical
config, source-manifest, event, and table hashes are the path-independent
semantic identities.

Key report identities:

```text
history count                         684
event point count                   4,306
event-set digest
  e2bca3cd9b0080735546f58afe1c545901ec19d75b3b1efe41409fd06537533f
primary source/human table digest
  a6c4a1df9d6589da1fb8643faa5f2fa35916ef18918c97ba49a29a5520027979
all mapped annotation digest
  6024c3cbb1328f06541f0f6b7085227b4214e564dd5e5812b1e9b481fbe57889
controlled-projection table digest
  3da6310e07746411cbfc41bed91c3ca0b004f94e1fcab4f87b5e2d792d011adf
exact-state table digest
  978a08033a2905fbb5e2b84d6deadcba28da9afd604e4f0c1dcf0f3538a1f99f
```

The functional Hall scaffold reported:

```text
source-semantic false invalidation  325 / 1,826
source-semantic false retention       0 / 2,480
exact structured agreement         4,306 / 4,306
direct-witness baseline unavailable 4,306 / 4,306 points
model calls                           0
```

## 5. Mandatory evidence boundary

The report's baseline 5 is still:

```text
execution backend
  pure-affected-group-hall-recompute-scaffold-v1
measurement validity
  functional_protocol_only
target runtime performance validated
  false
timing mode
  not_measured
```

Therefore none of its Hall work, logical state bytes, certificate bytes, or
semantic rates is maintained-overlay, sparse-publication, PostgreSQL, or
production-latency evidence. It does not close M5.5-05 through M5.5-07 and
does not make M5.5 complete. Wave C must run the same hash-bound histories
through actual M5 events, REQUIREMENT-only observation currency, maintained
Hall state, sparse publication, and independent Python/SQL oracles.

The result is controlled/retrospective because M3 already used WiCE. No fresh
blinded independently adjudicated cohort exists. Real-world or population
utility claims remain prohibited, and the human study remains explicit M6
debt.

## 6. Validation

Full focused evaluation suite with the pinned-source gate enabled:

```bash
GROUNDLOOP_WICE_SOURCE_ROOT=/home/kassym/Desktop/groundloop/models/m3/verifier-run-20260718/raw/wice_repo \
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src:tests \
/home/kassym/Desktop/groundloop/.venv/bin/python -m pytest -ra \
  tests/m5/evaluation -p no:cacheprovider
```

```text
41 passed in 6.65s
```

Static and repository checks:

```text
Ruff format check       1 file already formatted
Ruff evaluation scope  All checks passed!
strict mypy             Success: no issues found in 12 source files
compileall              exit 0
git diff --check        exit 0
```

No acceptance-matrix status is changed here because top-level status and
integration remain coordinator-owned.
