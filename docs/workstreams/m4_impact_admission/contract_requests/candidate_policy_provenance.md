# Contract request: complete candidate-policy index provenance

Status: blocking Wave 1 policy-manifest implementation

Date: 2026-07-19

## Frozen-contract conflict

`docs/m4_design_freeze.md` M4-13 requires every candidate-policy manifest to
record:

- lexical-v1 config hash;
- PostgreSQL version;
- regconfig identity;
- claim-registry statistics snapshot; and
- claim count.

M4-12 also requires HNSW recall to be measured for frozen build/search
parameters. The current coordinator-owned `CandidatePolicyManifest` in
`src/groundloop/m4/contracts.py` has only lexical/vector method-version strings
and no fields capable of preserving those frozen inputs explicitly.

Encoding all values into a free-form `lexical_method_version` or
`vector_method_version` would satisfy neither inspectability nor typed conflict
detection and would make policy-manifest tests vacuous.

## Minimal failing example

The following two policies are currently representable by identical shared
DTO payloads even though the freeze says they are distinct:

```text
policy A:
  lexical config hash = a...
  PostgreSQL = 16.14
  registry snapshot = registry-1
  claim count = 100

policy B:
  lexical config hash = b...
  PostgreSQL = 17.x
  registry snapshot = registry-2
  claim count = 500
```

The same defect applies to exact-vector versus HNSW execution and to different
HNSW build/search configurations if both reuse one method-version label.

## Proposed minimal shared signature change

Add these required fields to `CandidatePolicyManifest`:

```python
lexical_config_hash: str
lexical_postgres_version: str
lexical_regconfig_identity: str
claim_registry_snapshot_id: str
claim_count: int
vector_index_kind: str
vector_index_build_config_hash: str
vector_search_config_hash: str
```

Validation:

- hash fields are lowercase SHA-256;
- text identities are nonempty;
- `claim_count >= 0`;
- `vector_index_kind` is an enum or a frozen nonempty identifier such as
  `exact` or `hnsw`;
- `policy_hash` is derived from canonical serialization of every manifest
  field, or a shared builder validates the supplied hash.

If the coordinator prefers a nested immutable `CandidateIndexProvenance` DTO,
the manifest may contain that object instead, provided all fields participate
in policy identity.

## Compatibility impact

This is additive but constructor-breaking for the coordinator's current M4
contract tests and any lane fixtures created against the tagged baseline. No
M1-M3 schema or D-1 through D-20 semantic decision changes. No migration has
yet consumed the M4 manifest, so this is the cheapest point to correct it.

## Acceptance tests

1. Changing any lexical config hash, PostgreSQL version, regconfig identity,
   registry snapshot or claim count changes/invalidates policy identity.
2. Changing exact/HNSW mode or either vector configuration hash changes/
   invalidates policy identity.
3. Invalid hashes, blank identities and negative claim counts are rejected.
4. Canonically identical payloads produce identical policy hashes regardless
   of runtime file paths or dictionary insertion order.
5. The admission lane can construct one fake exact-vector manifest and one
   HNSW measurement manifest without overloading method-version strings.

## Requested coordinator action

Patch the shared contract and publish a replacement M4 contract-baseline
commit/tag. The admission lane will rebase only on coordinator instruction and
will not define a competing lane-local manifest.
