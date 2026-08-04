# M5.5 controlled evaluation lane

Date: 2026-08-03

Lane status: **GO for integration of the pure M5.5 adapter/protocol slice;
NO_GO for claiming the integrated M5.5 systems experiment or a fresh semantic
validation result.**

## Implemented scope

The lane owns only the M5 evaluation package, tests, script, configs, and this
workstream documentation. It implements:

- a strict typed controlled-evaluation config loader that rejects duplicate,
  missing, unknown, drifted, non-finite, and unsafe manifest-path values;
- config-local source-manifest resolution that enforces the frozen WiCE
  revision metadata and verifies six JSONL files, README, and license by exact
  byte count and SHA-256 before parsing;
- the byte-frozen `wice-evidence-unit-v1` adapter with an external immutable
  member relation, atomic sets, original zero-based ordinals, exact rejects,
  content-only JSON chunks, content-hash SDR deduplication, and least-ordinal
  projection;
- separate source/human, controlled-projection, exact-system-state, and
  optional frozen-model-diagnostic record types and report sections;
- exact independent matching and assignment-count mechanics for the bounded
  retrospective mapping;
- deterministic WiCE deletion/source-loss histories plus small authored
  histories for duplicate content, alternative witnesses, alternative groups,
  final-witness loss, matching-only loss, direct support, refutation conflict,
  supersession, replacement, and policy changes;
- all seven frozen baseline roles, identical event IDs/hashes, explicit
  applicability and `UNAVAILABLE`, and honest zero model-call counters;
- false-invalidation/retention raw numerators and denominators, full four-state
  status agreement, exact work counters for the pure comparators, canonical
  logical-state/certificate bytes, and deterministic 10,000-resample
  percentile intervals with seed `20260802` and NumPy PCG64;
- pair-specific fixed-size cluster draws for paired baseline differences. Each
  paired resample contains exactly the eligible pair cluster count and uses one
  draw for both comparator and reference.

WiCE projections are exclusively `REQUIREMENT` subjects with task type
`verify_requirement_v1`. They enter as one-hot SUPPORT observations through
`ObserveRequirementEvent`. The WiCE primary route creates zero claim-subject
observations and zero direct support. Baseline 3 is therefore explicitly
`UNAVAILABLE` for all 4,306 WiCE history points.

## Record scopes

The aggregate report avoids the earlier ambiguous mixing of full-source and
primary-cohort counts:

- `source_human.scope=primary representable cohort` covers 684 claims, 1,796
  requirements, their valid original source annotations, and 684 dynamic
  source-semantic states;
- `all_mapped_valid_annotation_count` separately covers all valid annotations
  derived from all mapped source rows, including rows outside the primary
  cohort; and
- controlled projections, exact SDR state, and optional model diagnostics
  each have a different schema and digest.

No source text is emitted in aggregate reports. Source/human labels never
enter semantic-observation currency, and optional model diagnostics cannot
select a cohort, policy, threshold, prompt, or implementation.

## Frozen baseline protocol and measurement validity

| # | Baseline | WiCE | Pure backend in this lane | Measurement validity |
|---:|---|---|---|---|
| 1 | Source invalidation | required | policy simulator | functional protocol only |
| 2 | Frozen direct citation | required | policy simulator | functional protocol only |
| 3 | Direct witness | `UNAVAILABLE` without independent whole-claim units | policy simulator | functional protocol only |
| 4 | Non-distinct conjunction | required | policy simulator with the identical direct-support disjunct | functional protocol only |
| 5 | GroundLoop Hall/SDR | required | affected-group Hall recomputation scaffold | **functional protocol only; latency disabled** |
| 6 | Affected-group full matching | required | independent pure-Python matching | controlled Python comparator |
| 7 | All-group full recomputation | required | independent pure-Python matching | controlled Python comparator |

Baseline 5 in this branch is not the maintained Hall-mask overlay. Its backend
is explicitly
`pure-affected-group-hall-recompute-scaffold-v1`, every result is marked
`functional_protocol_only`, and it cannot store a latency value even when
Python comparator timing is requested. Therefore its numbers are protocol and
semantic checks, not GroundLoop target-algorithm performance evidence.

Baseline 6/7 edge checks and rematch counters describe exactly the pure
algorithms executed here. Optional latency measures only those labelled Python
backends. `state_bytes` and `certificate_bytes` are canonical UTF-8 JSON
logical serializations. They are not Python heap/RSS, PostgreSQL storage, WAL,
or network bytes.

## Reproduction

Downloaded data remains outside Git:

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src \
python3 scripts/m5/run_controlled_evaluation.py \
  --source-root /tmp/groundloop-wice-source \
  --config configs/m5/controlled_evaluation_v1.json \
  --output /tmp/groundloop-m5-controlled-evaluation.json
```

`--manifest` is intentionally unavailable: the source manifest is resolved
only from the strict config and must remain inside the config directory. The
frozen config's canonical JSON SHA-256 is
`4ae7e27b9ada58965cbb72037739006d4caa728f955c27caf0bcc93bbee7532f`.
Reports and the CLI summary record that hash plus the resolved consumed config
and manifest paths. Frozen revision metadata is enforced and exact consumed
bytes are verified against manifest SHA-256 before parsing. The runtime loader
does not inspect or claim to inspect Git HEAD.

Primary-gate classification requires both the checked-in config hash above and
the checked-in manifest hash
`c4a186f42252c280b5cafdc26b182e88c5aec14072866af15dd86d4b4c4722f2`.
Fixture or alternate bundles that satisfy the structural freeze remain useful
for tests, but reports classify them as `non_primary_config_bundle`; they are
not frozen-primary result evidence.

Before config identity was added to report bytes, the deterministic no-latency
run was executed twice with byte-identical output. That earlier report produced
684,796 bytes and SHA-256
`06b2fc123cffa6e6ce99e91aa39d671cf30429a15f57dd7a82cf7fe5f5438158`.
That byte hash is retained as historical evidence, not the hash of the current
path-bearing report schema.

The manifest hash was
`c4a186f42252c280b5cafdc26b182e88c5aec14072866af15dd86d4b4c4722f2`;
the 684 WiCE histories contained 4,306 event points with event-set digest
`e2bca3cd9b0080735546f58afe1c545901ec19d75b3b1efe41409fd06537533f`.

The functional Hall scaffold reported raw retrospective counts:

- false invalidation: 325 / 1,826 = 0.1779846659, paired-cluster percentile
  interval [0.1553028727, 0.2022536803];
- false retention: 0 / 2,480 = 0, interval [0, 0]; and
- exact four-state agreement with the evaluation oracle: 4,306 / 4,306 = 1.

These are policy/scaffold results on generated deterministic histories over a
retrospective mapping. They are not maintained-kernel latency results,
real-model quality, independently adjudicated natural-history evidence, or a
population estimate.

## Remaining integration dependency

This lane could not truthfully execute the frozen baseline-5 target algorithm
because its branch does not contain the maintained M5 Hall-mask overlay/runtime
composition. It also does not materialize the generated group/chunk/event
records into the M5 repository/PostgreSQL path or run the independent SQL
oracle. Coordinator integration must:

1. rebase this lane onto the landed matching/runtime contracts;
2. translate the typed controlled group/chunk/projection records into the
   shared runtime input contract without bypassing observation currency;
3. run the same event IDs/hashes through the maintained baseline 5 and the
   Python/SQL oracles;
4. replace the functional baseline-5 backend with an explicitly runtime-valid
   backend before recording target latency/work; and
5. preserve this lane's applicability, estimand, provenance, and zero-call
   reporting contracts.

No fresh blinded two-annotator/adjudicated cohort or frozen-model diagnostic
was run. M5 can close only as implementation complete with
controlled/retrospective semantic evidence after the runtime gate passes. The
independent human study remains M6 debt.
