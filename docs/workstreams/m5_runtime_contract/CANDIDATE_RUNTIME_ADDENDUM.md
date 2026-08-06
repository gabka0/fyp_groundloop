# GroundLoop M5.4 Byte-Total Runtime Contract Addendum

Status: frozen runtime contract revision 3; implementation authorization
**GO** after the M5.3 014 schema bundle is integrated and validated

Date: 2026-08-03; revision 3 / M5-D21 and M5-D22 amendments 2026-08-06

Authority: this addendum specializes `docs/m5_design_freeze.md` M5-D1 through
M5-D22 and M5-T1/M5-T2. It does not change those decisions. The M5 design
freeze remains authoritative for semantic truth; this addendum is authoritative
for M5.4 runtime DTOs, identities, transition boundaries, persistence
ownership, replay, and acceptance tests.

## 1. Verdict and release boundary

The M5.4 runtime design is **GO with high confidence**. Implementation MUST
start only after migration 014 and its SQL oracle bundle have passed the M5.3
fresh-install, populated-upgrade, compatibility, and three-oracle gates.

This addendum MUST NOT authorize a change to an M5.0 semantic decision. An
implementation conflict with this addendum and M5-D1 through M5-D22 MUST stop
M5.4 as **NO-GO**. The exact amendment procedure MUST be a new numbered M5
decision in `docs/m5_design_freeze.md`, a matching acceptance-matrix row, and a
new runtime-addendum revision before code resumes. The migration-014 M4-open
guard conflict discovered during implementation is resolved only by M5-D21 and
the exact exception in Section 16. The missing changed-state artifact recipe
discovered during activation implementation is resolved only by M5-D22 and
Section 10.1; no unresolved conflict is present in this revision.

Normative wire values in backticks MUST be exact lowercase UTF-8. Every DTO in
this document MUST be immutable. Every tuple MUST use the order stated here.
Every identifier and present text field MUST be nonempty. Every SHA-256 value
MUST contain exactly 64 lowercase hexadecimal characters. An implementation
MUST NOT trim, case-fold, normalize, stringify with `repr`, serialize with
ambient JSON defaults, or substitute an empty string for NULL when constructing
an M5 v2 identity.

## 2. Composition boundary: M4-v1 CLAIM plus M5-v2 REQUIREMENT

### 2.1 Two logical subgraphs, one typed event

An activated typed event MUST contain these two disjoint logical subgraphs:

| Subgraph | Subject | Identities and artifacts | Persistence |
|---|---|---|---|
| direct | `claim` | the exact frozen M4-v1 pair, job, attempt, discovery, verifier, observation, currency, certificate, and publication recipes | the existing M4 relations and claim-filtered shared currency relations |
| requirement | `requirement` | the exact M5-v2 recipes in M5-D14 and this addendum | the parallel M5-v2 runtime relations in migration 015 and the M5 semantic-core relations in migration 014 |

The direct subgraph MUST preserve `PairKey`, `LogicalJobSpec`,
`JobCompletion`, M4 admission/artifact identities,
the frozen `m4-logical-job-v1` recipe, the frozen
`m4-semantic-observation-v1` recipe, and every frozen M4 state transition
byte-for-byte. M5.4 MUST NOT edit
`src/groundloop/m4/contracts.py` or
`src/groundloop/m4/models/contracts.py`.

The requirement subgraph MUST use `SemanticPairKey` with
`subject_kind=requirement`, the three M5-v2 job kinds, the M5 typed digest
expansion, canonical requirement task `verify_requirement_v1`, and parallel
runtime tables. It MUST NOT write a requirement job into
`groundloop_semantic_job` or reinterpret an M4-v1 claim job as typed v2.

The typed dispatcher MUST stage the base document/group mutation, exact direct
withdrawal, exact requirement withdrawal, direct M4 root declarations,
requirement M5 root declarations, both PENDING projections, and the durable
epoch in one structural-open transaction. It MUST NOT invoke
`M4Application.run_event()` or a public M4 opener that commits before the M5
overlay is durable.

For a typed document event, the declaration order inside that transaction MUST
be the held runtime-mode/publication-head locks, `groundloop_epoch`,
`groundloop_m5_update`, the revision-1 `groundloop_m5_runtime_epoch` header,
and then `groundloop_m4_update`. The M4 insert is admitted only through the
M5-D21 typed-sidecar-aware guard. Group/requirement-only typed events have no
direct declaration and MUST NOT synthesize an M4 update.

Direct and requirement worker completions MUST commit only working/audit state.
Neither subgraph MUST advance a publication head, emit a public delta, or make
strict state visible independently. The typed seal MUST promote both subgraphs,
write the combined states and certificates, emit the net combined deltas,
advance the global M4 head and M5 head to the same epoch, and mark the epoch
SEALED in one transaction.

### 2.2 Compatibility modes

`groundloop_runtime_mode.mode` MUST have exactly two values:
`v1_only` and `m5_active`.

On a schema-upgraded database whose mode is `v1_only`, feature-disabled
document insert/delete/replace MUST use the unmodified public M4-v1 dispatcher.
Its event payload hash, `OpenEventReceipt`, `PublicationReceipt`, publication
ID, `EventRunResult`, deltas, job IDs, artifact IDs, reconnect behavior, and
replay bytes MUST remain unchanged.

On a database whose mode is `m5_active`, every new mutation open or resume MUST
use the typed dispatcher. The public v1 dispatcher MUST reject the open/resume
before inserting an epoch or consuming an event ID. Read-only audit and exact
replay of preactivation v1 history MUST remain available. Every shared M4 read
that assumes claims MUST filter `subject_kind='claim'`.

Existing document/policy event payloads routed through the typed dispatcher
MUST retain their exact M4-v1 payload digest. Register/replace/retire group and
observe-requirement events MUST retain the exact M5-D2 payload digests. The
outer typed runtime sidecar MUST use M5-v2 job/result identities regardless of
whether the activated database currently has zero active groups.

`OpenEventReceipt` and `PublicationReceipt` MUST remain the exact existing M4
types. The typed publication ID MUST remain
`stable_m4_digest("m4-publication-v1", str(epoch_id))`. M5 state references and
the M5 logical run-result digest MUST be sidecars; they MUST NOT alter either
receipt.

## 3. Typed primitives and the frozen normalizer artifact

All new digests MUST use `stable_m5_digest` and the exact `TEXT`, `NULL`,
`INT`, `BOOL`, `ENUM`, `HASH`, `F64`, `SEQ`, and `OPTION` expansion in M5-D1.
`INT` MUST reject booleans. `F64` MUST reject nonfinite values and MUST encode
the exact big-endian IEEE-754 binary64 bits. Sequences MUST carry their length
and MUST contain already typed members.

The runtime MUST expose one immutable normalizer provenance value:

```text
M5TextNormalizerProvenance(
  normalizer_id="m5-normalize-text-v1",
  whitespace_codepoints=(
    9,10,11,12,13,28,29,30,31,32,133,160,5760,
    8192,8193,8194,8195,8196,8197,8198,8199,8200,8201,8202,
    8232,8233,8239,8287,12288
  ),
  boundary_rule="remove-boundary-runs",
  internal_rule="collapse-internal-runs-to-u+0020",
  other_codepoint_rule="preserve-exactly",
  unicode_normalization_rule="none",
  encoding="utf-8",
  hash_algorithm="sha256"
)
```

Its hash MUST be:

```text
normalizer_provenance_hash = stable_m5_digest(
  "m5-text-normalizer-provenance-v1",
  *TEXT(normalizer_id),
  *SEQ(INT(codepoint) for codepoint in whitespace_codepoints),
  *TEXT(boundary_rule), *TEXT(internal_rule),
  *TEXT(other_codepoint_rule), *TEXT(unicode_normalization_rule),
  *TEXT(encoding), *TEXT(hash_algorithm))
```

The tuple MUST contain exactly the 29 ascending code points above. Requirement
text and M5 witness chunk hashes MUST use this provenance. The stored legacy
chunk hash MUST remain bound separately and MUST NOT define M5 witness
identity. The recipe MUST produce
`d91b94f256f79c6bc7b29fafa41c7a608b90c17bd479b29a64be00fa538c49fb`.

## 4. Exact enums

The following enums MUST have exactly these wire values:

```text
M5DiscoveryDirection:
  forward_requirement | reverse_chunk

M5RequirementAdmissionChannel:
  lexical | lineage | vector

M5RetrievalTermination:
  budget_filled | snapshot_exhausted

M5JobKind:
  forward_requirement_retrieval
  reverse_requirement_discovery
  verify_requirement_pair

M5JobState:
  declared | running | completed_active | completed_inactive
  retryable_failed | terminal_failed | cancelled

M5ScopeState:
  open | result_staged | closed_active | closed_inactive
  terminal_failed | cancelled

M5TerminalReason:
  chunk_inactive | subject_inactive | epoch_failed | scope_retired
  retry_exhausted | retrieval_error | verifier_error | invalid_artifact

M5AttemptDisposition:
  root_result_staged | verifier_completed_active
  verifier_completed_inactive | terminal_audit_only

M5AttemptArchiveReason:
  epoch_failed | subject_inactive | chunk_inactive | job_already_terminal

M5RunState:
  sealed | failed | blocked | replayed

M5ReplayedOutcome:
  sealed | failed

M5RunFailureReason:
  retrieval_unavailable | verifier_unavailable | retry_exhausted
  retrieval_error | verifier_error | invalid_artifact | invariant_failure

M5StateReferenceKind:
  requirement_state | group_state | group_certificate
  claim_state | claim_certificate | answer_state
```

`M5JobState.terminal` MUST be true exactly for `completed_active`,
`completed_inactive`, `terminal_failed`, and `cancelled`.

## 5. Frozen M5-D14 identities retained without change

The implementation MUST use the exact M5-D14 recipes and domain tags for:

- `semantic_pair_digest` / `m5-semantic-pair-v2`;
- `requirement_registry_snapshot_digest` /
  `m5-requirement-registry-snapshot-v2`;
- `active_chunk_snapshot_digest` / `m5-active-chunk-snapshot-v2`;
- `scope_contract_digest` / `m5-discovery-scope-contract-v2`;
- `scope_closure_digest` / `m5-discovery-scope-closure-v2`;
- `payload_hash` / `m5-job-payload-v2`;
- `logical_job_id` / `m5-logical-job-v2`;
- `child_set_hash` / `m5-child-set-v2`; and
- `completion_digest` / `m5-job-completion-v2`.

`M5CandidatePolicyManifest` MUST contain exactly the fields and use exactly the
`m5-candidate-policy-v2` hash recipe in M5-D14. Both budgets MUST be positive.
`candidate_policy_id` MUST name the immutable manifest hash and MUST NOT enter
that content hash.

The payload's role and execution hashes MUST use this total table:

| Job kind | role_template_hash | execution_spec_hash |
|---|---|---|
| `forward_requirement_retrieval` | manifest `requirement_role_template_hash` | `forward_retrieval_execution_spec_hash` below |
| `reverse_requirement_discovery` | manifest `chunk_role_template_hash` | `reverse_retrieval_execution_spec_hash` below |
| `verify_requirement_pair` | `requirement_verifier_role_binding_hash` below | manifest `verifier_execution_spec_hash` |

```text
forward_retrieval_execution_spec_hash = stable_m5_digest(
  "m5-forward-requirement-retrieval-execution-v2",
  *HASH(candidate_policy_manifest_hash),
  *HASH(normalizer_provenance_hash))

reverse_retrieval_execution_spec_hash = stable_m5_digest(
  "m5-reverse-requirement-discovery-execution-v2",
  *HASH(candidate_policy_manifest_hash),
  *HASH(normalizer_provenance_hash))

requirement_verifier_role_binding_hash = stable_m5_digest(
  "m5-requirement-verifier-role-binding-v2",
  *HASH(requirement_role_template_hash),
  *HASH(chunk_role_template_hash))
```

A root MUST NOT substitute the opposite direction's role/execution hash. A
verifier MUST NOT use either single retrieval role hash as its pair binding.

`RequirementRegistrySnapshotEntry` MUST contain, in this order:
`requirement_version_id`, `group_version_id`, `group_family_id`,
`owner_claim_id`, `normalized_requirement_text`, and
`requirement_text_hash`. Entries MUST be sorted by
`requirement_version_id`. `requirement_count` MUST equal tuple length.

`ActiveChunkSnapshotEntry` MUST contain, in this order:
`chunk_version_id` and `text_hash`. Entries MUST be sorted by
`chunk_version_id`. `chunk_count` MUST equal tuple length. `text_hash` MUST
equal SHA-256 of the exact chunk text after the normalizer in Section 3.

The total direction, root, pair, parent, and expandable table in M5-D14 MUST
remain exact. Both-present and both-NULL scope targets MUST be rejected.
Every v2 selected pair and verifier child MUST have
`subject_kind=requirement`.

## 6. Immutable admission and discovery DTOs

### 6.1 Semantic pair

```text
SemanticPairKey(
  subject_kind: SubjectKind,
  subject_id: str,
  chunk_version_id: str
)
```

M5.4 runtime construction MUST accept only `subject_kind=requirement`. The
subject MUST occur in the bound requirement snapshot and the chunk MUST occur
in the bound active-chunk snapshot. `semantic_pair_digest` MUST equal the
frozen M5-D14 recipe.

### 6.2 Requirement channel hit

```text
M5RequirementChannelHit(
  epoch_id: int,
  root_job_id: str,
  scope_contract_digest: SHA256,
  pair: SemanticPairKey,
  semantic_pair_digest: SHA256,
  candidate_policy_id: str,
  channel: M5RequirementAdmissionChannel,
  rank: int,
  score: float | None,
  channel_artifact_hash: SHA256,
  hit_digest: SHA256
)
```

`epoch_id` and `rank` MUST be positive. `semantic_pair_digest` MUST match
`pair`. Vector and lexical hits MUST carry a finite `score`; lineage hits MUST
carry NULL. Within one root, `(channel, rank)` and `(channel,
semantic_pair_digest)` MUST each be unique. Ranks within each nonempty channel
MUST be the dense sequence `1..n`. A channel-hit tuple MUST be sorted by
`(channel.value, rank, semantic_pair_digest)`.

`hit_digest` MUST be:

```text
stable_m5_digest(
  "m5-requirement-channel-hit-v2",
  *INT(epoch_id), *TEXT(root_job_id), *HASH(scope_contract_digest),
  *HASH(semantic_pair_digest), *TEXT(candidate_policy_id), *ENUM(channel),
  *INT(rank), *OPTION(F64(score)), *HASH(channel_artifact_hash))
```

### 6.3 Per-scope selection

The accepted `fusion_version` MUST be exactly `rank-interleave-v1`. Each vector
and lexical channel MUST be requested to the direction-specific manifest
budget. Within a channel, higher score MUST rank first and an exact tie MUST
use target ID ascending by UTF-8 bytes: chunk ID for forward retrieval and
requirement ID for reverse discovery. ANN replay MUST use persisted hits/ranks
and MUST NOT assume an index rebuild reproduces them.

Approximate fusion MUST consume one vector hit then one lexical hit in repeated
rounds, skip a pair already selected without assigning another fused rank, and
stop after the direction budget of unique approximate pairs or after both
channels are exhausted. Vector MUST precede lexical in every round. Lineage
MUST NOT consume the approximate budget. Unique lineage-only pairs MUST be
appended after approximate fusion in ascending semantic-pair-digest order.

```text
M5RequirementScopeSelection(
  root_job_id: str,
  scope_contract_digest: SHA256,
  pair: SemanticPairKey,
  semantic_pair_digest: SHA256,
  fused_rank: int,
  reasons: tuple[M5RequirementAdmissionChannel, ...],
  mandatory_lineage: bool,
  selection_digest: SHA256
)
```

`fused_rank` MUST be positive. `reasons` MUST be nonempty, sorted by enum wire
value, and unique. `mandatory_lineage` MUST equal membership of `lineage` in
`reasons`. Every reason MUST have a matching channel hit for the same root and
pair. Scope selections MUST be sorted by `(fused_rank,
semantic_pair_digest)` and fused ranks MUST be the dense sequence `1..n`.

`selection_digest` MUST be:

```text
stable_m5_digest(
  "m5-requirement-scope-selection-v2",
  *TEXT(root_job_id), *HASH(scope_contract_digest),
  *HASH(semantic_pair_digest), *INT(fused_rank),
  *SEQ(ENUM(reason) for reason in reasons), *BOOL(mandatory_lineage))
```

### 6.4 Successful discovery result

```text
M5RequirementDiscoveryResult(
  root_job_id: str,
  scope_contract_digest: SHA256,
  termination: M5RetrievalTermination,
  channel_hits: tuple[M5RequirementChannelHit, ...],
  selections: tuple[M5RequirementScopeSelection, ...],
  approximate_selection_count: int,
  mandatory_lineage_only_count: int,
  result_artifact_hash: SHA256,
  result_artifact_id: SHA256
)
```

The root and scope on every nested row MUST equal the outer values. Every
selected pair MUST have at least one hit. An unselected hit MUST remain
provenance only and MUST NOT become a reserve. `approximate_selection_count`
MUST equal the number of selections having `vector` or `lexical` among their
reasons. `mandatory_lineage_only_count` MUST equal the number having only the
`lineage` reason. Both MUST be nonnegative and their sum MUST equal selection
tuple length. The approximate count MUST NOT exceed the direction-specific
manifest budget. A lineage-only selection MUST exist only when
`lineage_safety_override=true`; it MUST be appended after approximate fusion
in ascending semantic-pair-digest order and MUST be charged outside the
approximate budget. `budget_filled` MUST require approximate count equal to
that budget. `snapshot_exhausted` MUST require approximate count strictly less
than that budget and MUST assert that all eligible rows in the frozen snapshot
were exhausted. An empty or short `snapshot_exhausted` result MUST be a
successful result. Retrieval unavailability, timeout, adapter error, or retry
exhaustion MUST NOT construct this DTO.

`result_artifact_hash` MUST be:

```text
stable_m5_digest(
  "m5-requirement-discovery-result-v2",
  *TEXT(root_job_id), *HASH(scope_contract_digest), *ENUM(termination),
  *SEQ(HASH(hit.hit_digest) for hit in channel_hits),
  *SEQ(HASH(selection.selection_digest) for selection in selections),
  *INT(approximate_selection_count), *INT(mandatory_lineage_only_count))
```

`result_artifact_id` MUST be:

```text
stable_m5_digest(
  "m5-requirement-discovery-artifact-v2",
  *TEXT(root_job_id), *HASH(result_artifact_hash))
```

### 6.5 Event-wide admitted pair

```text
M5RequirementAdmittedPairSource(
  root_job_id: str,
  scope_contract_digest: SHA256,
  selection_digest: SHA256
)

M5RequirementAdmittedPair(
  epoch_id: int,
  pair: SemanticPairKey,
  semantic_pair_digest: SHA256,
  candidate_policy_id: str,
  owner_root_job_id: str,
  sources: tuple[M5RequirementAdmittedPairSource, ...],
  reasons: tuple[M5RequirementAdmissionChannel, ...],
  mandatory_lineage: bool,
  admitted_pair_digest: SHA256
)
```

`sources` MUST contain every successful scope selection for that event/pair
and MUST be sorted by `root_job_id`. Source root IDs MUST be unique.
`owner_root_job_id` MUST be the lexicographically least source root ID by UTF-8
byte order. `reasons` MUST be the sorted unique union of every source
selection reason. `mandatory_lineage` MUST equal membership of `lineage` in
that union. Exactly one admitted-pair row MUST exist per
`(epoch_id, semantic_pair_digest, candidate_policy_id)`.

`admitted_pair_digest` MUST be:

```text
stable_m5_digest(
  "m5-requirement-admitted-pair-v2",
  *INT(epoch_id), *HASH(semantic_pair_digest),
  *TEXT(candidate_policy_id), *TEXT(owner_root_job_id),
  *SEQ(SEQ((TEXT(source.root_job_id), HASH(source.scope_contract_digest),
            HASH(source.selection_digest))) for source in sources),
  *SEQ(ENUM(reason) for reason in reasons), *BOOL(mandatory_lineage))
```

## 7. Immutable verifier input, result, and observation contracts

### 7.1 Pair input

```text
M5RequirementPairInput(
  pair: SemanticPairKey,
  semantic_pair_digest: SHA256,
  scope_contract_digest: SHA256,
  candidate_policy_id: str,
  owner_claim_id: str,
  group_version_id: str,
  group_family_id: str,
  requirement_ordinal: int,
  normalized_requirement_text: str,
  requirement_text_hash: SHA256,
  document_version_id: str,
  chunk_index: int,
  chunk_text: str,
  stored_chunk_text_hash: SHA256,
  m5_chunk_text_hash: SHA256,
  chunker_artifact_id: str,
  normalizer_id: str,
  normalizer_provenance_hash: SHA256,
  pair_input_hash: SHA256
)
```

The pair MUST be a requirement pair. `requirement_ordinal` and `chunk_index`
MUST be nonnegative. Ownership/group/ordinal/text fields MUST exactly match the
immutable requirement snapshot member and migration-014 group relations.
`normalized_requirement_text` MUST equal M5 normalization of itself and MUST
be nonempty. `requirement_text_hash` MUST equal SHA-256 of its exact UTF-8
bytes. `chunk_text` MUST be the exact immutable chunk-row text, without strip
or rendering changes, and its M5-normalized value MUST be nonempty.
`stored_chunk_text_hash` MUST equal the immutable legacy chunk-row value.
`m5_chunk_text_hash` MUST equal SHA-256 of M5-normalized exact chunk text and
MUST equal the bound active-chunk snapshot member. `normalizer_id` and
`normalizer_provenance_hash` MUST equal the constants in Section 3.
`chunker_artifact_id` MUST match the immutable chunk provenance row.

`pair_input_hash` MUST be:

```text
stable_m5_digest(
  "m5-requirement-pair-input-v2",
  *HASH(semantic_pair_digest), *HASH(scope_contract_digest),
  *TEXT(candidate_policy_id), *TEXT(owner_claim_id),
  *TEXT(group_version_id), *TEXT(group_family_id),
  *INT(requirement_ordinal), *TEXT(normalized_requirement_text),
  *HASH(requirement_text_hash), *TEXT(document_version_id),
  *INT(chunk_index), *TEXT(chunk_text), *HASH(stored_chunk_text_hash),
  *HASH(m5_chunk_text_hash), *TEXT(chunker_artifact_id),
  *TEXT(normalizer_id), *HASH(normalizer_provenance_hash))
```

### 7.2 Verifier artifact

```text
M5RequirementVerifierArtifact(
  artifact_id: SHA256,
  artifact_hash: SHA256,
  pair: SemanticPairKey,
  semantic_pair_digest: SHA256,
  pair_input_hash: SHA256,
  execution_spec_hash: SHA256,
  model_artifact_id: str,
  model_id: str,
  model_revision: str,
  prompt_artifact_id: str,
  prompt_version: str,
  calibration_version: str,
  calibration_artifact_hash: SHA256 | None,
  temperature: float,
  decision_policy_version: str,
  decision_policy_hash: SHA256,
  support_score: float,
  refute_score: float,
  neutral_score: float,
  raw_logits: tuple[float, float, float],
  raw_output_hash: SHA256,
  operational_label: VerificationLabel
)
```

The pair and pair-input identities MUST agree with the verifier job. All six
numeric values MUST be finite. Scores MUST each lie in `[0,1]`. The IEEE-754
evaluation `abs(((support_score + refute_score) + neutral_score) - 1.0)` MUST
be at most `1e-6`. `temperature` MUST be positive. `raw_logits` MUST contain
exactly three values in model order `(contradiction, entailment, neutral)`;
stored scores MUST use order `(support=entailment, refute=contradiction,
neutral)`. `operational_label` MUST equal the result of the bound
decision-policy version/hash; it MUST NOT be accepted from an unchecked model
label.

`artifact_hash` MUST be:

```text
stable_m5_digest(
  "m5-requirement-verifier-result-v2",
  *HASH(semantic_pair_digest), *HASH(pair_input_hash),
  *HASH(execution_spec_hash), *TEXT(model_artifact_id), *TEXT(model_id),
  *TEXT(model_revision), *TEXT(prompt_artifact_id), *TEXT(prompt_version),
  *TEXT(calibration_version), *OPTION(HASH(calibration_artifact_hash)),
  *F64(temperature), *TEXT(decision_policy_version),
  *HASH(decision_policy_hash), *F64(support_score), *F64(refute_score),
  *F64(neutral_score), *SEQ(F64(value) for value in raw_logits),
  *HASH(raw_output_hash), *ENUM(operational_label))
```

`artifact_id` MUST be:

```text
stable_m5_digest(
  "m5-requirement-verifier-artifact-v2",
  *HASH(semantic_pair_digest), *HASH(pair_input_hash), *HASH(artifact_hash))
```

### 7.3 Semantic observation

The canonical observation task MUST be the exact text
`verify_requirement_v1`. The observation ID MUST be:

```text
observation_id = stable_m5_digest(
  "m5-requirement-semantic-observation-v2",
  *HASH(verifier_artifact_id), *TEXT("verify_requirement_v1"))
```

The archived `SemanticObservation` MUST have
`subject_kind=requirement`, pair subject/chunk IDs, artifact scores, artifact
model ID/revision/prompt version, task `verify_requirement_v1`, and
`input_hash=pair_input_hash`. Its execution row MUST bind the verifier artifact
ID/hash, job ID, attempt ID, decision-policy identity, and exact pair input.
Only an eligible current SUPPORT observation from this task MUST enter
`ActiveRequirementWitness`. REFUTE and NEUTRAL MUST remain auditable and MUST
NOT create direct or parent-claim refutation.

## 8. Exact v2 job, completion, attempt, and late-result DTOs

### 8.1 Logical job

```text
M5LogicalJobSpec(
  logical_job_id: SHA256,
  structural_event_id: str,
  job_kind: M5JobKind,
  candidate_policy_id: str,
  candidate_policy_manifest_hash: SHA256,
  parent_job_id: SHA256 | None,
  pair: SemanticPairKey | None,
  semantic_pair_digest: SHA256 | None,
  scope_contract_digest: SHA256 | None,
  requirement_registry_snapshot_digest: SHA256,
  active_chunk_snapshot_digest: SHA256,
  role_template_hash: SHA256,
  execution_spec_hash: SHA256,
  expandable: bool,
  payload_hash: SHA256
)
```

The fields MUST validate against the exact M5-D14 kind/nullability table.
`pair` and `semantic_pair_digest` MUST be jointly NULL or jointly present.
Every verifier MUST carry its enclosing `scope_contract_digest`. Every root
MUST carry its scope contract and NULL pair. Parent MUST be NULL for a root
and MUST be the enclosing root for a verifier. `payload_hash` and
`logical_job_id` MUST equal the frozen M5-D14 recipes.

### 8.2 Completion

```text
M5JobCompletion(
  logical_job_id: SHA256,
  payload_hash: SHA256,
  execution_spec_hash: SHA256,
  terminal_state: M5JobState,
  result_artifact_id: SHA256 | None,
  result_artifact_hash: SHA256 | None,
  scope_closure_digest: SHA256 | None,
  child_set_hash: SHA256 | None,
  archive_reason: M5TerminalReason | None,
  completion_digest: SHA256
)
```

The nullability and reason subsets MUST equal M5-D14 exactly:

- `completed_active` MUST carry result ID/hash and NULL reason. An expandable
  completion MUST carry closure/child hashes; a verifier MUST carry both NULL.
- `completed_inactive` MUST carry result ID/hash and exactly one of
  `chunk_inactive`, `subject_inactive`, or `epoch_failed`. An expandable
  completion MUST carry the canonical empty closure and empty child-set hash;
  a verifier MUST carry both closure fields NULL.
- `cancelled` MUST carry NULL result/closure fields and exactly one of
  `subject_inactive`, `scope_retired`, or `epoch_failed`.
- `terminal_failed` MUST carry NULL result/closure fields and exactly one of
  `retry_exhausted`, `retrieval_error`, `verifier_error`, or
  `invalid_artifact`.

`completion_digest` MUST equal the exact frozen
`m5-job-completion-v2` recipe. A nonterminal state MUST be rejected.

### 8.3 Attempt lease and worker output

```text
M5JobAttempt(
  attempt_id: SHA256,
  logical_job_id: SHA256,
  attempt_ordinal: int,
  execution_spec_hash: SHA256,
  lease_token_hash: SHA256
)
```

`attempt_ordinal` MUST be positive and dense per job. `attempt_id` MUST be:

```text
stable_m5_digest(
  "m5-job-attempt-v2", *TEXT(logical_job_id),
  *INT(attempt_ordinal), *HASH(execution_spec_hash))
```

The opaque lease token MUST remain outside semantic identity. Only its hash
MUST be durable.

```text
M5AttemptOutput(
  attempt_id: SHA256,
  logical_job_id: SHA256,
  job_epoch_id: int,
  payload_hash: SHA256,
  execution_spec_hash: SHA256,
  result_artifact_id: SHA256,
  result_artifact_hash: SHA256,
  attempt_output_digest: SHA256
)
```

`attempt_output_digest` MUST be:

```text
stable_m5_digest(
  "m5-attempt-output-v2", *TEXT(attempt_id), *TEXT(logical_job_id),
  *INT(job_epoch_id), *HASH(payload_hash), *HASH(execution_spec_hash),
  *HASH(result_artifact_id), *HASH(result_artifact_hash))
```

The attempt ID MUST be the idempotency key for worker return. The first return
MUST reserve `(attempt_id, attempt_output_digest)`. An exact digest replay MUST
return the stored receipt with zero writes and zero epoch revision change. The
same attempt ID with another digest MUST conflict and MUST change no row.

### 8.4 Persisted attempt-result artifact

```text
M5AttemptResultArtifact(
  attempt_result_artifact_id: SHA256,
  attempt_result_artifact_hash: SHA256,
  attempt_output_digest: SHA256,
  attempt_id: SHA256,
  logical_job_id: SHA256,
  job_epoch_id: int,
  job_state_at_receipt: M5JobState,
  job_state_after: M5JobState,
  disposition: M5AttemptDisposition,
  activity_snapshot_epoch_id: int,
  activity_snapshot_revision: int,
  epoch_active: bool,
  chunk_active: bool | None,
  requirement_active: bool | None,
  group_active: bool | None,
  archive_reason: M5AttemptArchiveReason | None,
  cancelled_by_event_id: str | None,
  cancelled_by_epoch_id: int | None,
  cancellation_reason: M5TerminalReason | None
)
```

Epoch IDs MUST be positive and revisions MUST be nonnegative. Applicability of
activity fields MUST follow job shape:

| Job | chunk_active | requirement_active | group_active |
|---|---|---|---|
| reverse root | present | NULL | NULL |
| forward root | NULL | present | present |
| verifier | present | present | present |

`root_result_staged` MUST keep a root job `running`; it MUST be the only
disposition accepted for a successful root output before the event-wide
closure barrier. It MUST carry NULL `archive_reason` when every applicable
activity flag is true and otherwise MUST carry the highest-precedence inactive
reason; the barrier MUST then install a canonical empty inactive closure.
`verifier_completed_active` MUST transition a running verifier to
`completed_active` and MUST carry NULL `archive_reason`.
`verifier_completed_inactive` MUST transition a running verifier to
`completed_inactive` and MUST carry the exact inactive reason.
`terminal_audit_only` MUST leave an already terminal job unchanged.

The inactivity classifier MUST use this total precedence:

```text
if the original job epoch is FAILED: epoch_failed
else if an applicable requirement or parent group is inactive: subject_inactive
else if an applicable chunk is inactive: chunk_inactive
else: job_already_terminal
```

The classifier MUST evaluate all applicable predicates at the one serialized
activity snapshot recorded in the artifact. It MUST NOT stop after the first
database read. A still-running inactive verifier MUST use the first three
reasons only. A terminal audit-only return MUST use all four.

The three cancellation-attribution fields MUST be jointly present exactly
when `job_state_at_receipt=cancelled`; otherwise they MUST all be NULL. They
MUST name the event and epoch that performed the original cancellation, not
the later activity snapshot. This attribution MUST survive a later sealed
group replacement or chunk replacement. A stale worker from the prior epoch
MUST therefore remain tied to its original cancellation while the activity
flags record the later snapshot.

`attempt_result_artifact_hash` MUST be:

```text
stable_m5_digest(
  "m5-attempt-result-artifact-v2",
  *HASH(attempt_output_digest), *TEXT(attempt_id), *TEXT(logical_job_id),
  *INT(job_epoch_id), *ENUM(job_state_at_receipt), *ENUM(job_state_after),
  *ENUM(disposition), *INT(activity_snapshot_epoch_id),
  *INT(activity_snapshot_revision), *BOOL(epoch_active),
  *OPTION(BOOL(chunk_active)), *OPTION(BOOL(requirement_active)),
  *OPTION(BOOL(group_active)), *OPTION(ENUM(archive_reason)),
  *OPTION(TEXT(cancelled_by_event_id)),
  *OPTION(INT(cancelled_by_epoch_id)),
  *OPTION(ENUM(cancellation_reason)))
```

`attempt_result_artifact_id` MUST be:

```text
stable_m5_digest(
  "m5-attempt-result-id-v2", *TEXT(attempt_id),
  *HASH(attempt_result_artifact_hash))
```

An audit-only late result MUST insert only its immutable attempt-result and
referenced immutable worker artifact. It MUST NOT change the original epoch
revision, job state, scope state, open-work count, blocking-failure count,
PENDING counter, currency, witness edge, matching state, certificate, working
state, published state, or publication head.

## 9. Activation singleton, receipt, replay, and route barrier

Migration 014 MUST own the exact relations `groundloop_runtime_mode`,
`groundloop_m5_activation`, and `groundloop_m5_publication_head`. Migration 015
MUST consume them and MUST NOT recreate or alter them.

`groundloop_m5_activation` MUST contain exactly the semantic/audit columns
`singleton`, `activation_id`, `payload_hash`, `base_m4_epoch_id`, and
`activated_at`. `singleton` MUST be the true primary key, `activation_id` MUST
be unique and nonblank, `payload_hash` MUST be a lowercase SHA-256 digest,
`base_m4_epoch_id` MUST reference `groundloop_epoch`, and `activated_at` MUST be
the database audit timestamp. The row MUST be immutable.

The immutable activation request MUST be:

```text
M5ActivationRequest(
  activation_id: str,
  expected_mode_revision: int,
  expected_base_m4_epoch_id: int,
  expected_m4_publication_id: str,
  core_schema_bundle_sha256: SHA256,
  bootstrap_state_hash: SHA256,
  payload_hash: SHA256
)
```

`expected_mode_revision` MUST be nonnegative and the base epoch MUST be
positive. `expected_m4_publication_id` MUST equal
`stable_m4_digest("m4-publication-v1", str(expected_base_m4_epoch_id))`.
`core_schema_bundle_sha256` MUST equal the successfully ledgered 014 two-file
bundle hash. `bootstrap_state_hash` MUST equal the canonical changed-state-set
hash independently computed from the base-head bootstrap input. `payload_hash`
MUST be:

```text
stable_m5_digest(
  "m5-activation-request-v2", *TEXT(activation_id),
  *INT(expected_mode_revision), *INT(expected_base_m4_epoch_id),
  *TEXT(expected_m4_publication_id), *HASH(core_schema_bundle_sha256),
  *HASH(bootstrap_state_hash))
```

The semantic receipt MUST be:

```text
M5ActivationReceipt(
  activation_id: str,
  payload_hash: SHA256,
  base_m4_epoch_id: int,
  m4_publication_id: str,
  m5_publication_epoch_id: int,
  mode_revision: int,
  bootstrap_state_hash: SHA256,
  receipt_hash: SHA256,
  replayed: bool
)
```

`m5_publication_epoch_id` MUST equal `base_m4_epoch_id` and `mode_revision`
MUST equal the request's expected revision plus one. `bootstrap_state_hash`
MUST equal the request value and the `m5-changed-state-set-v2` hash over every bootstrapped
group/claim/answer state and certificate reference sorted as in Section 10.
`receipt_hash` MUST be:

```text
stable_m5_digest(
  "m5-activation-receipt-v2", *TEXT(activation_id), *HASH(payload_hash),
  *INT(base_m4_epoch_id), *TEXT(m4_publication_id),
  *INT(m5_publication_epoch_id), *INT(mode_revision),
  *HASH(bootstrap_state_hash))
```

`replayed` MUST NOT enter the receipt hash. The first valid activation MUST
insert the immutable singleton row, bootstrap all M5 published states and
certificate bindings at the existing M4 head, insert the M5 head at that same
epoch, and change mode to `m5_active` in one transaction. It MUST NOT create a
synthetic activation epoch.

The same activation ID and payload hash MUST replay with the same semantic
receipt and `replayed=true`, with zero writes and no mode revision change. The
same ID with another payload, or another activation ID after activation, MUST
conflict. `groundloop_m5_activation.activated_at` MUST remain an audit timestamp
and MUST NOT enter any semantic digest.

## 10. Changed-state references, work records, and M5EventRunResult

### 10.1 Changed-state reference

```text
M5ChangedStateReference(
  kind: M5StateReferenceKind,
  object_id: str,
  epoch_id: int,
  revision: int,
  state_artifact_hash: SHA256,
  reference_digest: SHA256
)
```

`reference_digest` MUST be:

```text
stable_m5_digest(
  "m5-changed-state-reference-v2", *ENUM(kind), *TEXT(object_id),
  *INT(epoch_id), *INT(revision), *HASH(state_artifact_hash))
```

For semantic-state kinds, `state_artifact_hash` MUST use exactly one of these
recipes over the complete persisted state payload. Publication coordinates do
not enter these inner hashes because `epoch_id` and `revision` are already
bound by the outer reference. Every sequence shown below MUST already be
unique and sorted under the frozen state contract.

```text
stable_m5_digest(
  "m5-requirement-state-artifact-v2", *TEXT(requirement_version_id),
  *SEQ(HASH(witness_hash) for witness_hash in witness_hashes),
  *SEQ(TEXT(observation_id) for observation_id in supporting_observation_ids),
  *INT(witness_count), *BOOL(satisfied), *TEXT(decision_policy_version))

stable_m5_digest(
  "m5-group-state-artifact-v2", *TEXT(group_version_id),
  *INT(requirement_count), *INT(satisfied_count), *INT(matching_size),
  *BOOL(complete), *TEXT(decision_policy_version),
  *OPTION(HASH(certificate_digest)))

stable_m5_digest(
  "m5-claim-state-artifact-v2", *TEXT(claim_id), *INT(support_count),
  *INT(refute_count), *OPTION(F64(best_support_score)),
  *OPTION(F64(best_refute_score)),
  *SEQ(TEXT(observation_id) for observation_id in supporting_observation_ids),
  *SEQ(TEXT(observation_id) for observation_id in refuting_observation_ids),
  *INT(complete_group_count),
  *SEQ(TEXT(group_version_id) for group_version_id in complete_group_ids),
  *ENUM(status), *TEXT(decision_policy_version), *HASH(certificate_digest))

stable_m5_digest(
  "m5-answer-state-artifact-v2", *TEXT(answer_version_id),
  *INT(required_claim_count), *INT(supported_count),
  *INT(unsupported_count), *INT(refuted_count), *INT(conflicted_count),
  *ENUM(status))
```

For `group_certificate` and `claim_certificate`, `state_artifact_hash` MUST
equal the referenced immutable artifact's existing `certificate_digest`
exactly. It MUST NOT be digested again. The artifact tables' existing frozen
recipes remain authoritative for certificate bytes.

References MUST be sorted by `(kind.value, object_id, reference_digest)` and
unique by `(kind, object_id)`. The set hash MUST be:

```text
stable_m5_digest(
  "m5-changed-state-set-v2",
  *SEQ(HASH(reference.reference_digest) for reference in references))
```

The set MUST include full-state or certificate-only changes even when no
public status enum changes.

### 10.2 Exact work counters

```text
M5RuntimeWork(
  deactivated_chunk_count: int,
  withdrawn_candidate_edge_count: int,
  withdrawn_current_observation_count: int,
  direct_discovery_call_count: int,
  direct_verifier_call_count: int,
  direct_observation_artifact_count: int,
  direct_effective_observation_count: int,
  direct_inactive_completion_count: int,
  requirement_forward_retrieval_call_count: int,
  requirement_reverse_retrieval_call_count: int,
  requirement_fallback_forward_call_count: int,
  requirement_verifier_call_count: int,
  requirement_observation_artifact_count: int,
  requirement_effective_observation_count: int,
  requirement_inactive_completion_count: int,
  requirement_cancelled_job_count: int,
  requirement_late_attempt_artifact_count: int,
  requirement_channel_hit_count: int,
  requirement_pre_dedup_selection_count: int,
  requirement_admitted_pair_count: int,
  group_state_write_count: int,
  claim_state_write_count: int,
  answer_state_write_count: int,
  certificate_binding_write_count: int,
  public_delta_count: int,
  bytes_hashed: int,
  bytes_serialized: int,
  embedding_model_call_count: int,
  verifier_model_call_count: int,
  embedding_input_token_count: int,
  verifier_input_token_count: int,
  verifier_output_token_count: int,
  work_digest: SHA256
)
```

Every counter MUST be a nonnegative integer. A reused immutable model artifact
MUST increment no model-call counter. An attempted external call MUST increment
its model-call counter exactly once even when it returns an error. Pre-dedup
selection count MUST count all per-scope selections; admitted-pair count MUST
count the event-wide unique rows.

`work_digest` MUST use domain `m5-runtime-work-v2` followed by every integer
above, in the exact displayed order and excluding `work_digest` itself, each
encoded with `INT`.

```text
M5RuntimeTiming(
  coordinator_non_db_non_neural_ns: int,
  neural_wall_ns: int,
  postgres_roundtrip_wall_ns: int,
  external_io_wall_ns: int,
  end_to_end_wall_ns: int,
  postgres_server_execution_ns: int | None,
  postgres_lock_wait_ns: int | None,
  postgres_wal_bytes: int | None,
  postgres_shared_block_reads: int | None
)
```

Every present timing/physical value MUST be nonnegative. Server-only values
MUST be NULL when the database did not expose them. Component wall times MUST
be reported independently and MUST NOT be asserted to sum to end-to-end time.
Timing values MUST NOT enter any semantic identity.

### 10.3 Combined delta binding

The typed result MUST reuse the existing immutable `StatusDelta` fields. Deltas
MUST contain only claim and answer objects, MUST be sorted by
`(object_type, object_id)`, and MUST contain at most one net delta per object.
Their set hash MUST be:

```text
stable_m5_digest(
  "m5-combined-status-delta-set-v2",
  *SEQ(SEQ((TEXT(delta.event_id), TEXT(delta.object_type),
            TEXT(delta.object_id), TEXT(delta.old_status),
            TEXT(delta.new_status), TEXT(delta.reason))) for delta in deltas))
```

### 10.4 Run result and replay

```text
M5EventRunResult(
  event_id: str,
  payload_hash: SHA256,
  epoch_id: int,
  state: M5RunState,
  replayed_outcome: M5ReplayedOutcome | None,
  open_receipt: OpenEventReceipt,
  publication_receipt: PublicationReceipt | None,
  event_work: M5RuntimeWork,
  call_work: M5RuntimeWork,
  event_timing: M5RuntimeTiming,
  call_timing: M5RuntimeTiming,
  combined_deltas: tuple[StatusDelta, ...],
  changed_state_references: tuple[M5ChangedStateReference, ...],
  failure_reason: M5RunFailureReason | None,
  logical_result_hash: SHA256 | None
)
```

`sealed` MUST require an open receipt with both terminal flags false, a
non-replayed publication receipt, NULL replayed outcome, NULL failure, and a
logical result hash. Its open receipt `replayed` flag MUST be false for the
opening invocation and true for a reconnect that resumed committed nonterminal
work. `failed` MUST require an open receipt with both terminal flags false,
NULL publication, NULL replayed outcome, a failure reason, and a logical result
hash. `blocked` MUST require an open receipt with both terminal flags false,
NULL publication, NULL replayed outcome, a reason other than
`invariant_failure`, and NULL logical result hash. A blocked retryable job MUST
remain resumable under the typed dispatcher. A blocked terminal-failure job
MUST require an explicit atomic epoch-failure decision; it MUST NOT retry or
seal.

`replayed` MUST require `replayed_outcome`. A sealed replay MUST carry
`OpenEventReceipt(replayed=true, already_sealed=true,
publication_id=stored_publication_id)` and
`PublicationReceipt(replayed=true)`. A failed replay MUST carry
`OpenEventReceipt(replayed=true, already_failed=true,
failure_reason=stored_failure_reason)` and
NULL publication. A replay MUST return the stored event work, deltas, state
references, failure outcome, and logical result hash. Its `call_work` MUST be
all zero. It MUST perform zero discovery, embedding, verifier, mutation, delta,
or publication writes. Replay lookup reads and their timing MUST remain in
`call_timing`.

The open-receipt sidecar binding MUST hash the exact receipt fields with domain
`m5-open-event-receipt-binding-v2` in this order: epoch `INT`, replayed `BOOL`,
already-sealed `BOOL`, publication ID `OPTION(TEXT)`, already-failed `BOOL`,
failure reason `OPTION(TEXT)`. The publication-receipt sidecar binding MUST use
domain `m5-publication-receipt-binding-v2` and fields epoch `INT`, publication
ID `TEXT`, replayed `BOOL`.

The durable logical result MUST retain the first structural-open receipt
binding with `replayed=false`, both terminal flags false, NULL publication, and
NULL failure. It MUST retain the original publication binding with
`replayed=false` for a sealed outcome. A reconnect/open receipt and returned
terminal replay receipt MUST be validated against those original identities
after applying only the state-dependent projection required in Section 10.4.

`logical_result_hash` MUST exclude receipt replay booleans, `state`,
`replayed_outcome`, `call_work`, and all timing. It MUST be:

```text
stable_m5_digest(
  "m5-event-run-logical-result-v2", *TEXT(event_id), *HASH(payload_hash),
  *INT(epoch_id), *ENUM(sealed_or_failed_outcome),
  *HASH(original_open_receipt_binding_hash),
  *OPTION(HASH(original_publication_receipt_binding_hash)),
  *HASH(event_work.work_digest),
  *HASH(combined_status_delta_set_hash), *HASH(changed_state_set_hash),
  *OPTION(ENUM(failure_reason)))
```

The durable event-result row MUST be inserted in the same transaction that
seals or terminally fails the epoch. Exact reconnect replay MUST read this row
and MUST NOT reconstruct logical deltas from current mutable state.

## 11. Bounded forward/reverse retrieval and fallback

### 11.1 Root declaration set

The typed structural-open transaction MUST declare the complete M5 root set.
No M5 root MUST be appended after that commit.

```text
M5RequirementFallbackKey(
  requirement_version_id: str,
  candidate_policy_id: str
)

M5RequirementWithdrawalPlan(
  event_id: str,
  deactivated_chunk_version_ids: tuple[str, ...],
  withdrawn_candidate_pair_digests: tuple[SHA256, ...],
  withdrawn_observation_ids: tuple[str, ...],
  cancelled_job_ids: tuple[SHA256, ...],
  fallback_keys: tuple[M5RequirementFallbackKey, ...],
  plan_digest: SHA256
)
```

Every tuple MUST be sorted and unique; fallback keys MUST be sorted by
`(requirement_version_id, candidate_policy_id)`. The plan MUST be produced
only from exact stored reverse edges and post-event activity. `plan_digest`
MUST be:

```text
stable_m5_digest(
  "m5-requirement-withdrawal-plan-v2", *TEXT(event_id),
  *SEQ(TEXT(value) for value in deactivated_chunk_version_ids),
  *SEQ(HASH(value) for value in withdrawn_candidate_pair_digests),
  *SEQ(TEXT(value) for value in withdrawn_observation_ids),
  *SEQ(TEXT(value) for value in cancelled_job_ids),
  *SEQ(SEQ((TEXT(key.requirement_version_id),
            TEXT(key.candidate_policy_id))) for key in fallback_keys))
```

- Every newly registered requirement version MUST receive exactly one
  `forward_requirement_retrieval` root against the event's frozen active-chunk
  snapshot. Its successful approximate selection count MUST be at most
  `forward_budget_per_requirement`.
- Every inserted chunk version MUST receive exactly one
  `reverse_requirement_discovery` root against the event's frozen active-
  requirement snapshot. Its successful approximate selection count MUST be at most
  `reverse_budget_per_inserted_chunk`.
- Exact deletion withdrawal MUST compute fallback keys before root declaration.
  For each distinct active `(requirement_version_id, candidate_policy_id)`
  touched by a withdrawn M5 candidate edge or current requirement-observation
  edge, the event MUST declare at most one fresh forward root. Multiple
  deactivated chunks touching the same key MUST coalesce into that one root.
  A forward root already required for the same new requirement/key MUST be the
  one root and MUST NOT be duplicated.

The phrase "at most one fresh forward scope" MUST be interpreted per
`(event_id, requirement_version_id, candidate_policy_id)`, not per deleted
chunk and not as one global event-wide requirement limit.

Withdrawal MUST enumerate persisted reverse candidate and current-observation
edges. It MUST NOT invoke vector, lexical, embedding, verifier, or model work.
Retired/replaced old requirements MUST be withdrawn and cancelled; they MUST
NOT receive fallback roots. Successor requirements MUST receive their normal
new-version forward roots.

### 11.2 No hidden reserve

M5.4 MUST NOT maintain, promote, or consult an unverified reserve. Raw channel
hits not selected by fusion MUST remain provenance only. A selected pair MUST
become an admitted pair and required verifier child after event-wide dedup. A
nonselected hit MUST enter a later event only through a new frozen scope and a
new discovery result.

A successful empty or short result with
`termination=snapshot_exhausted` MUST close normally and MUST remove its
PENDING contribution. It MUST leave the requirement/group incomplete under
the candidate policy when no witness exists. It MUST NOT synthesize REFUTE,
degraded success, or a reserve.

External retrieval unavailability MUST leave the root open in
`retryable_failed` and MUST return a blocked run result. Retrieval retry
exhaustion or a durable retrieval/invalid-artifact failure MUST transition the
root to `terminal_failed`, MUST increment the blocking-failure counter, and
MUST block seal. Neither case MUST be converted to an empty successful result.

### 11.3 Durable latest forward frontier

```text
M5RequirementFrontierHead(
  requirement_version_id: str,
  candidate_policy_id: str,
  latest_root_job_id: SHA256,
  latest_scope_contract_digest: SHA256,
  latest_active_chunk_snapshot_digest: SHA256,
  latest_discovery_result_artifact_hash: SHA256,
  latest_scope_closure_digest: SHA256,
  latest_completion_digest: SHA256,
  completed_epoch_id: int,
  completed_revision: int
)
```

Exactly one current head MUST exist after the first successful active forward
closure for a frontier key. Empty and short successful closures MUST update the
head. Reverse roots MUST NOT update it. A cancelled, inactive, retryable, or
failed forward root MUST NOT replace it. The closure-barrier transaction MUST
update the head atomically with the forward root completion. Replay MUST
validate the complete head payload and MUST perform no update.

`latest_discovery_result_artifact_hash` MUST bind the forward root's complete
pre-dedup top-k-plus-lineage selection. `latest_scope_closure_digest` MUST bind
the post-dedup pairs owned by that root. Both MUST be retained so event-wide
dedup cannot erase the audited forward result when another root owns the one
verifier child.

The latest head and its bound active-chunk snapshot MUST be the only persisted
forward-frontier baseline. Fallback MUST run a fresh forward scope against the
post-withdrawal snapshot; it MUST NOT reinterpret the prior selected set as a
complete current top-k.

## 12. Event-wide pair deduplication and atomic root closure

The structural-open transaction MUST persist the sorted complete requirement
root ID tuple and this hash:

```text
requirement_root_set_hash = stable_m5_digest(
  "m5-requirement-root-set-v2",
  *SEQ(TEXT(root_job_id) for root_job_id in sorted unique root IDs))
```

An event with zero requirement roots MUST persist the canonical empty root-set
hash, MUST create no closure-barrier transaction, and MUST leave every M5
open/scope/failure/PENDING counter at zero. It MUST still use the activated
typed result and seal path.

A successful root attempt MUST persist its discovery result and attempt-result
artifact, then move its scope `open -> result_staged` while the root job remains
`running`. It MUST NOT insert children, admitted pairs, a scope closure, a
completion digest, or a frontier head at that stage.

The closure barrier MUST become eligible only when every root in the frozen
root set has one validated staged result. An inactive root MUST stage its
artifact but MUST contribute the canonical empty selection at closure. A
retryable root MUST keep the barrier ineligible. A terminal failure MUST block
the event; it MUST NOT permit partial closure of the other roots.

One CAS transaction MUST perform all of these operations in this exact logical
order:

1. lock and validate the epoch, root-set hash, every root, every scope, every
   staged result, and the expected epoch revision;
2. materialize the union of active staged scope selections;
3. group by `(semantic_pair_digest, candidate_policy_id)` and build exactly one
   `M5RequirementAdmittedPair` per group;
4. choose the lexicographically least UTF-8 root job ID as owner;
5. insert every admitted pair and every verifier child owned by each root;
6. compute each root's scope closure from only its owned admitted pair digests;
7. validate a bijection between each closure pair digest and child job;
8. install every child set, scope closure, root completion, dependency edge,
   and successful forward-frontier head;
9. replace broad/forward root PENDING contributions with verifier-child
   contributions; and
10. increment the shared epoch revision exactly once.

No root completion MUST commit before this transaction. A pair selected by
multiple forward/reverse/fallback roots MUST create one admitted row and one
verifier job. Nonowner roots MUST retain their source selection provenance but
MUST exclude that pair from their closure. Empty owner sets MUST use the
canonical empty `scope_closure_digest` and `child_set_hash`.

The barrier completion hash MUST be:

```text
stable_m5_digest(
  "m5-requirement-root-barrier-completion-v2",
  *TEXT(structural_event_id), *HASH(requirement_root_set_hash),
  *SEQ(SEQ((TEXT(root_job_id), HASH(result_artifact_hash)))
       for roots sorted by root_job_id),
  *SEQ(HASH(admitted_pair_digest)
       for admitted pairs sorted by semantic_pair_digest))
```

Exact barrier replay MUST compare this hash and every installed child/closure
identity, return a no-op, and preserve the epoch revision. Any result, owner,
child, closure, or head mismatch MUST conflict and roll back.

## 13. Open-work, blocking-failure, and owner-projected PENDING

### 13.1 Epoch counters

Every typed epoch MUST persist these independent counters:

```text
open_work_count
open_scope_count
blocking_failure_count
```

`open_work_count` MUST equal the number of M5 jobs in `declared`, `running`, or
`retryable_failed`. A result-staged root remains `running` and MUST remain in
this count. `open_scope_count` MUST equal scopes in `open` or `result_staged`.
`blocking_failure_count` MUST equal M5 jobs in `terminal_failed`.
`cancelled`, `completed_active`, and `completed_inactive` MUST enter none of
these counters.

`retryable_failed` MUST be open work and MUST NOT be a blocking failure.
`terminal_failed` MUST be a blocking failure and MUST NOT be open work. Every
transition MUST update its affected counters in the same CAS transaction.
Counters MUST never be negative. Measured seal MUST require all three to equal
zero and MUST read them by point lookup; it MUST NOT derive them with an
aggregate scan.

### 13.2 Owner multiplicity

For each typed epoch and owner claim, the runtime MUST persist:

```text
M5OwnerPendingCounter(
  epoch_id,
  owner_claim_id,
  broad_reverse_scope_count,
  forward_scope_count,
  verifier_job_count,
  blocking_failure_count
)
```

The counter's pending multiplicity MUST be the exact sum of those four
nonnegative integers.

- An open/result-staged reverse root MUST contribute one broad unit to each
  distinct owner claim represented in its frozen requirement snapshot. Two
  requirements with the same owner in one root MUST still contribute one unit.
- An open/result-staged forward root MUST contribute one forward unit to its
  one owner.
- After barrier closure, each nonterminal verifier child MUST contribute one
  verifier unit to its owner. Multiple pairs for one owner MUST retain their
  multiplicity.
- A job transition to `terminal_failed` MUST atomically move its one owner unit
  from the applicable open field to `blocking_failure_count`; total pending
  multiplicity MUST remain unchanged.
- Completion or cancellation MUST remove the applicable unit exactly once.
  A late audit-only attempt MUST remove nothing.

The combined claim evaluation state MUST be PENDING exactly when the existing
direct-M4 projection is pending or the M5 owner pending multiplicity is
positive, while the epoch remains active. Direct support, complete alternative
groups, or refutation MUST NOT mask PENDING.

For each required owner claim, every M5 owner contribution MUST project with
the same multiplicity to its owning answer. Optional owner claims MUST project
zero answer units. An answer MUST be PENDING exactly when its direct-M4 pending
projection or its summed required-owner M5 multiplicity is positive. A failed
epoch MUST expose FAILED rather than PENDING while retaining historical
counters for audit.

The root-closure barrier MUST remove broad/forward contributions and add child
contributions in one transaction. An empty closure MUST remove the root unit
without an intermediate COMPLETE/PENDING state. Cancellation and terminal
failure MUST update epoch counters, owner counters, answer counters, and job
state in the same transaction.

## 14. Atomic state machines and CAS rules

### 14.1 Epoch

The typed route MUST retain the frozen M4 epoch graph:

```text
RECEIVED -> STRUCTURAL_COMMITTED -> SEMANTIC_PENDING
         -> SEMANTIC_COMPLETE -> SEALED
any durable unsealed state -> FAILED
```

Structural open MUST assign revision 1. Every successful state-changing
runtime transaction MUST compare the expected revision and increment it once,
regardless of row count. Job acquisition, retryable failure, root-result
staging, event-wide root closure, verifier completion, cancellation batch,
terminal epoch failure, and seal MUST each be one such transaction. Exact
replay, conflict rejection, read-only reconnect, and audit-only late-attempt
archival MUST NOT change the epoch revision.

The epoch MUST enter SEMANTIC_COMPLETE only when the direct M4 coordination
surface is complete and the three M5 counters are zero. A blocking failure
MUST prevent SEMANTIC_COMPLETE. No model call MUST execute inside a database
transaction.

A cursor-local M4 transition may compute or temporarily write direct readiness
inside the typed transaction, but it is never the final state authority. Before
commit, the outer typed coordinator MUST overwrite/validate the shared
`groundloop_epoch.semantic_status` and `evaluation_state` from the combined
direct readiness and all three M5 counters at the same resulting revision.
Last-direct-job completion while any M5 counter is nonzero MUST therefore leave
the epoch pending; no direct-only complete state may commit or become visible.

### 14.2 Job and scope

The only normal job edges MUST be:

```text
declared -> running
running -> completed_active | completed_inactive | retryable_failed
retryable_failed -> running
running | retryable_failed -> terminal_failed
declared | running | retryable_failed -> cancelled
```

Root-result staging MUST leave the job `running` while the scope moves
`open -> result_staged`. The barrier MUST move a staged scope to
`closed_active` or `closed_inactive` and its root job to the corresponding
completed state. A root terminal failure MUST move its scope to
`terminal_failed`. A root cancellation MUST move its scope to `cancelled`.
No scope MUST reopen and no child MUST be appended after closure.

Job acquisition MUST lock the epoch/job, validate payload and execution hashes,
transition the state, insert the dense next attempt, persist the external-call
dispatch marker, update counters, and increment revision in one transaction.
The dispatch marker MUST commit before external work starts. Crash recovery
MUST count every actually dispatched attempt; a retried call MUST use a new
attempt ordinal.

### 14.3 Verifier active/inactive completion

A verifier-completion transaction MUST:

1. reserve and validate the attempt output;
2. lock the epoch, job, attempt, pair input, subject/group/chunk activity rows,
   currency key, affected matching/state keys, and PENDING counters;
3. compare the expected revision, lease token hash, payload hash, execution
   hash, artifact hash, pair input hash, and job state;
4. classify activity with the precedence in Section 8.4;
5. archive the immutable verifier artifact, observation, execution row, and
   attempt-result artifact;
6. for active completion only, install eligible currency, edge/matching
   changes, certificates, combined claim/answer patches, and working state;
7. terminalize the job, decrement open/PENDING once, and increment revision
   once.

Inactive completion MUST set `eligible_for_currency=false` and MUST perform no
currency, edge, matching, certificate, or derived-state mutation. Exact replay
MUST be a no-op. A differing attempt output MUST conflict. A return to an
already terminal job MUST use the audit-only transaction and MUST NOT enter
this state machine.

### 14.4 Cancellation and durable failure

Cancellation MUST be a sorted batch CAS over all selected open jobs/scopes.
It MUST record `cancelled_by_event_id`, `cancelled_by_epoch_id`, and the exact
terminal reason on every job. It MUST decrement open/PENDING contributions
exactly once, close affected scopes, and increment the epoch revision once.
Exact cancellation replay MUST validate the same selected set and reasons and
MUST be a no-op. A different selected set or reason MUST conflict.

`fail_typed_epoch_atomically` MUST lock both subgraphs and MUST make the epoch
FAILED in one transaction. It MUST cancel every remaining open M5 job with
`epoch_failed`, retain existing terminal-failure rows and counters as audit,
mark staged migration-014 group rows FAILED, invoke the transaction-local M4
failure projection, store the durable failed `M5EventRunResult`, and leave both
publication heads and all strict state unchanged.

### 14.5 Typed seal

Measured typed seal MUST use point/CAS persisted invariants only. It MUST check:

- expected epoch revision and SEMANTIC_COMPLETE;
- direct M4 open-job/scope/fallback and compact-evaluation readiness;
- M5 `open_work_count=open_scope_count=blocking_failure_count=0`;
- exact M4/M5 head equality with the event predecessor;
- no open working currency interval inconsistency;
- nonnegative refcounts and valid migration-014 group/claim certificates;
- valid combined claim/answer states and PENDING zero; and
- a complete immutable event work record and changed-state set.

In one transaction seal MUST invoke the transaction-local M4 structural,
currency, direct-state, evaluation, and sparse-publication subgraph; promote
the M5 group/currency/combined-state/certificate sidecar; emit one net public
delta per changed claim/answer; insert the changed-state references and durable
run result; advance `groundloop_m4_publication_head` and
`groundloop_m5_publication_head` to the same epoch; mark the epoch SEALED; and
return the unchanged M4 `PublicationReceipt`.

The measured seal MUST NOT call the Python oracle, SQL full-recompute oracle,
runtime pure-transition oracle, full repository hydration, or aggregate job
scan. Those oracles MUST run after transaction commit in audit mode. An audit
mismatch MUST fail the release gate and MUST NOT retroactively rewrite the
sealed snapshot.

### 14.6 Crash recovery and reconnect

Every named transaction MUST be failure-atomic under an injected exception
after each durable statement group. Reconnect MUST hydrate from committed rows,
not process-local caches. A staged root result MUST be reused by the barrier
without another retrieval/model call. A committed verifier completion MUST be
reused without another verifier call. A computed external result lost before
its transaction commits MUST be retried only through a new attempt and MUST
remain visible as an additional dispatched call in work accounting.

A reconnect to a nonterminal event MUST resume only missing work. A reconnect
to SEALED or FAILED MUST return exact replay from the durable event-result row
with zero model calls and zero writes. A crash at any seal injection point
MUST expose either the complete old publication or the complete new
publication, never one advanced head or one subgraph alone.

## 15. Total runtime lock order

Every runtime transaction MUST acquire only the tiers it needs and MUST acquire
them in this total order. It MUST NOT acquire an earlier tier after a later
tier. Rows within a tier MUST be locked by ascending numeric key and then
ascending UTF-8 byte order using `COLLATE "C"` for text.

1. `groundloop_runtime_mode` singleton;
2. `groundloop_m4_publication_head` singleton;
3. `groundloop_m5_publication_head` singleton;
4. `groundloop_m5_activation` singleton;
5. event-idempotency row, then `groundloop_epoch` row;
6. typed runtime-epoch header/counter row;
7. structural document, document-version, chunk-version, group-family,
   group-version, requirement-version, and deactivation-overlay rows;
8. requirement/chunk snapshot rows, then M5 scope rows;
9. direct M4 jobs, then M5 jobs, each by logical job ID;
10. attempts, attempt outputs/results, dependencies, discovery results,
    channel hits, selections, admitted pairs, and frontier heads;
11. semantic observations and working/current currency keys by
    `(subject_kind, subject_id, chunk_version_id, task_type)`;
12. requirement state, group state, and group-certificate bindings;
13. direct/combined claim state and claim-certificate bindings;
14. direct/combined answer state;
15. owner/answer PENDING counters and compact evaluation rows; and
16. public delta, changed-state-reference, event-result, and publication rows.

Activation MUST lock tiers 1 through 5 before bootstrap and MUST reject a live
epoch. A v1 opener MUST hold tier 1 from its mode check through durable epoch
insert. A typed opener MUST hold tiers 1 through 6 while checking active mode,
locking both heads, validating equality, and inserting the event/epoch.
Completion transactions MUST start at tier 5. Typed seal MUST start at tier 1.
Transaction-local M4 helpers MUST accept already-held earlier-tier locks and
MUST NOT open nested transactions.

## 16. Migration 015 boundary and dependency on 014

The runtime migration MUST be named exactly
`migrations/015_m5_runtime.sql`. It MUST execute only after the exact 014
schema-plus-oracle bundle has a successful immutable ledger row in
`groundloop_m5_schema_bundle`. Its installer MUST verify the expected 014
bundle ID/hash before executing any 015 DDL. Missing, mismatched, or unvalidated
014 evidence MUST abort 015 with no schema change.

Migration 014 MUST exclusively own these prerequisites consumed by 015:

- `groundloop_m5_schema_bundle`;
- `groundloop_runtime_mode`;
- `groundloop_m5_activation`;
- `groundloop_m5_publication_head`;
- `groundloop_semantic_subject` and typed shared observation/currency
  integrity;
- all group-family/group-version/requirement-version/lifecycle relations;
- `groundloop_m5_working_currency_history`;
- all M5 working/published requirement, group, claim, and answer state
  relations; and
- all group/claim certificate artifact and binding relations.

Except for the exact M5-D21 bridge below, migration 015 MUST NOT recreate,
rename, add a column to, weaken a constraint on, or change the semantics of any
object in that list. It MUST NOT alter an M4-v1 contract table or digest check.
Source changes that make the v1 opener honor the 014 runtime-mode barrier MUST
remain behavior-neutral in `v1_only`.

M5-D21 authorizes migration 015 to execute exactly one
`CREATE OR REPLACE FUNCTION` against a migration-014 object: the body of
`groundloop_m5_guard_v1_open()`. The existing
`groundloop_m4_update_runtime_mode_guard` trigger remains installed and
unchanged. The replacement function MUST:

1. retain the exact `v1_only` acceptance branch;
2. in `m5_active`, reject unless the same epoch already has a revision-1
   `structural_committed` `groundloop_m5_runtime_epoch` header and a
   `groundloop_m5_update` document declaration inserted by the current SQL
   transaction; the epoch, M5 update, and runtime header insertion-transaction
   identities MUST all equal the current transaction, and an earlier committed
   matching row MUST NOT pass;
3. require `groundloop_epoch.event_id` to equal the runtime header's structural
   event ID; map M4 `insert/delete/replace` exactly to M5
   `document_insert/document_delete/document_replace`; require identical prior
   publication epochs; require the M4 and runtime candidate-policy IDs to
   match; require the header's policy-manifest hash to equal the immutable M5
   policy row; require the M4 and M5 candidate-policy rows and M5 update to
   agree on decision-policy version; and require the M4 registry snapshot to
   equal its immutable M4 candidate-policy binding; and
4. reject every missing or mismatched sidecar without changing a row.

Migration 015 MUST add a deferred checked validation rooted on its new runtime
header. At commit, a document-kind typed declaration MUST have exactly the
matching M4 update described above, and a non-document typed declaration MUST
have none. This validation prevents a typed sidecar from being committed as a
reusable guard bypass. Migration 015 MUST NOT disable or defer the existing M4
guard, alter runtime mode during installation, or authorize any standalone M4
write in `m5_active`.

Public M4 resume, completion, failure, and seal entrypoints MUST reject any
epoch having a `groundloop_m5_runtime_epoch` header before changing a row.
Only cursor-local M4 helpers called by the typed coordinator under its existing
transaction may operate on that epoch's direct subgraph, and the typed outer
transaction retains combined failure/publication/seal authority.

Migration 015 MUST create exactly these runtime-owned relation families:

```text
groundloop_m5_candidate_policy
groundloop_m5_requirement_registry_snapshot
groundloop_m5_requirement_registry_snapshot_member
groundloop_m5_active_chunk_snapshot
groundloop_m5_active_chunk_snapshot_member
groundloop_m5_runtime_epoch
groundloop_m5_discovery_scope
groundloop_m5_requirement_channel_hit
groundloop_m5_requirement_scope_selection
groundloop_m5_requirement_discovery_result
groundloop_m5_requirement_admitted_pair
groundloop_m5_requirement_admitted_pair_source
groundloop_m5_semantic_job
groundloop_m5_job_dependency
groundloop_m5_job_attempt
groundloop_m5_attempt_result_artifact
groundloop_m5_requirement_pair_input
groundloop_m5_requirement_verifier_artifact
groundloop_m5_requirement_verifier_execution
groundloop_m5_requirement_frontier_head
groundloop_m5_owner_pending_counter
groundloop_m5_answer_pending_counter
groundloop_m5_runtime_work
groundloop_m5_event_result
groundloop_m5_event_result_delta
groundloop_m5_event_result_state_reference
```

The SQL checks and foreign keys MUST enforce every enum, nullable-shape,
digest-width, positive/nonnegative, parent/root, snapshot membership,
subject-kind, one-row, uniqueness, and terminal-state invariant in this
addendum. Cross-row child/closure bijection, event-wide owner selection,
counter equality, and result-set hashes MUST be validated by deferred
constraint triggers or transaction-local checked procedures; comments MUST
NOT stand in for enforcement.

All snapshot, policy, input, discovery, admitted-pair, verifier, attempt-result,
and sealed-result payload columns MUST be immutable. Job/scope state, lease,
counter, and frontier-head transitions MUST be accepted only through checked
procedures with expected-revision CAS. Direct `UPDATE` that bypasses those
procedures MUST be rejected by privileges or triggers.

The 015 file content MUST be bound as:

```text
runtime_schema_bundle_sha256 = stable_m5_digest(
  "m5-runtime-schema-bundle-v2",
  *TEXT("migrations/015_m5_runtime.sql"),
  *HASH(sha256_of_exact_015_file_bytes),
  *HASH(core_schema_bundle_sha256))
```

The installer MUST record this exact row in `groundloop_m5_schema_bundle`
atomically with DDL:

```text
bundle_id = "m5-runtime-schema-bundle-v2"
bundle_sha256 = runtime_schema_bundle_sha256
migration_sha256 = sha256_of_exact_015_file_bytes
oracle_sha256 = e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855
prerequisite_sha256 = core_schema_bundle_sha256
```

The `oracle_sha256` value MUST be SHA-256 of canonical empty bytes because 015
has no SQL-oracle bundle member. Exact rerun MUST be a no-op; same bundle ID
with another digest MUST fail. A crash or injected failure before commit MUST
leave neither partial DDL nor a ledger row.

015 installation MUST lock `groundloop_runtime_mode`, both publication heads,
and the epoch table in the order in Section 15, reject a live open epoch, run
DDL/backfill/validation, force deferred constraints immediate, and commit once.
It MUST leave mode unchanged and MUST NOT activate M5.

## 17. Exact implementation APIs and file ownership

### 17.1 New coordinator-owned files

The coordinator MUST own these files:

```text
src/groundloop/m5/runtime/__init__.py
src/groundloop/m5/runtime/contracts.py
src/groundloop/m5/runtime/digests.py
src/groundloop/m5/runtime/frontier.py
src/groundloop/m5/runtime/application.py
src/groundloop/m5/runtime/persistence.py
src/groundloop/m5/runtime/direct_m4.py
migrations/015_m5_runtime.sql
tests/m5/runtime/
tests/m5/postgres_runtime/
```

`contracts.py` MUST define the DTOs/enums in this addendum.
`digests.py` MUST contain only the new recipes and MUST import the frozen typed
primitives from `groundloop.m5.digests`. `frontier.py` MUST implement the pure
fallback/root-set and latest-head rules without persistence or models.
`application.py` MUST coordinate ports and MUST NOT own a database
transaction. `persistence.py` MUST own every checked PostgreSQL transaction.
`direct_m4.py` MUST expose the transaction-local M4-v1 subgraph adapter and
MUST NOT redefine M4 contracts.

The coordinator MUST update `src/groundloop/m5/__init__.py` only to export the
accepted public runtime types. It MUST update `src/groundloop/m4/pipeline.py`
and `src/groundloop/m4/persistence.py` only to extract transaction-local direct
helpers, add the runtime-mode open barrier, and add explicit claim filters.
Those changes MUST preserve every frozen M4 test and digest vector.

### 17.2 Public protocols

The activation API MUST be:

```text
M5ActivationPort.activate(
  request: M5ActivationRequest
) -> M5ActivationReceipt
```

The typed application API MUST be:

```text
M5TypedApplication.run_event(
  event: M5TypedEventPlan
) -> M5EventRunResult
```

```text
M5TypedEventPlan(
  structural_event_id: str,
  event: LegacyEvent | RegisterGroupEvent | ReplaceGroupEvent |
         RetireGroupEvent | ObserveRequirementEvent,
  payload_hash: SHA256,
  direct_plan: DynamicEventPlan | None,
  candidate_policy_id: str,
  candidate_policy_manifest_hash: SHA256,
  requirement_registry_snapshot: RequirementRegistrySnapshot,
  active_chunk_snapshot: ActiveChunkSnapshot,
  expected_previous_published_epoch_id: int
)
```

`payload_hash` MUST equal the frozen payload recipe for `event`.
`direct_plan` MUST be present exactly for a document insert/delete/replace and
MUST otherwise be NULL. A present direct plan MUST carry the same event ID,
payload hash, candidate-policy ID, and predecessor epoch. The two snapshots
MUST validate their frozen digests. Every component MUST name the same policy
and predecessor head.

The structural port MUST expose exactly:

```text
plan_exact_requirement_withdrawal(event) -> M5RequirementWithdrawalPlan
open_typed_event_atomically(
  event, direct_withdrawal, requirement_withdrawal,
  direct_roots, requirement_roots, requirement_root_set_hash
) -> OpenEventReceipt
```

The runtime persistence port MUST expose exactly:

```text
acquire_m5_job(epoch_id, expected_revision, job) -> M5JobLease
mark_m5_retryable_failure(epoch_id, expected_revision, lease, error_hash)
stage_m5_discovery_result_atomically(
  epoch_id, expected_revision, lease, job, result, attempt_output
) -> M5AttemptCompletionReceipt
close_m5_requirement_roots_atomically(
  epoch_id, expected_revision, requirement_root_set_hash
) -> M5RootBarrierReceipt
complete_m5_verifier_atomically(
  epoch_id, expected_revision, lease, job, pair_input,
  verifier_artifact, attempt_output
) -> M5AttemptCompletionReceipt
cancel_m5_work_atomically(
  epoch_id, expected_revision, cancellation_plan
) -> M5CancellationReceipt
fail_typed_epoch_atomically(
  epoch_id, expected_revision, failure_reason
) -> M5EventRunResult
request_typed_seal_atomically(
  epoch_id, expected_revision, event
) -> M5EventRunResult
read_typed_event_result(event_id, payload_hash) -> M5EventRunResult | None
```

Every mutating method MUST own exactly one transaction and MUST accept an
expected revision. `M5JobLease`, `M5AttemptCompletionReceipt`,
`M5RootBarrierReceipt`, and `M5CancellationReceipt` MUST contain the resulting
revision and an exact-replay boolean. The replay boolean MUST be false after a
state change and true only after complete payload validation with zero writes.

The direct-M4 transaction adapter MUST expose exactly:

```text
stage_direct_open(
  cursor, event: DynamicEventPlan, withdrawal: StructuralWithdrawal,
  roots: tuple[LogicalJobSpec, ...], scopes: tuple[DiscoveryScope, ...]
) -> OpenEventReceipt

stage_direct_expansion(
  cursor, epoch_id: int, expected_revision: int, lease: JobLease,
  discovery: DiscoveryResult, completion: JobCompletion,
  children: tuple[LogicalJobSpec, ...]
) -> None

stage_direct_verifier_completion(
  cursor, epoch_id: int, expected_revision: int, lease: JobLease,
  verifier_job: LogicalJobSpec, completion: JobCompletion,
  observation: SemanticObservation, make_effective: bool
) -> ObservationCompletionReceipt

stage_direct_failure(
  cursor, epoch_id: int, expected_revision: int, reason: str
) -> None

stage_direct_seal(
  cursor, epoch_id: int, expected_revision: int,
  update: CorpusUpdateIdentity
) -> PublicationReceipt
```

Each method MUST receive the typed transaction's cursor and held-lock context,
MUST execute the same SQL/state logic as the existing M4 path, and MUST NOT
commit, roll back, open a nested transaction, advance a head alone, or alter a
v1 DTO/digest.

## 18. Falsifying acceptance matrix

Every row below MUST pass before M5.4 is marked implemented. A skipped,
unavailable, no-match, or environment-failed row MUST remain non-PASS.

### 18.1 Unit and golden-contract tests

| Test surface | Falsifying requirement |
|---|---|
| typed digests | Golden vectors MUST cover every nullable branch, negative/zero/positive INT, `-0.0`, infinities/NaNs rejected, child/result permutations, empty sequences, and UTF-8 boundary collisions. |
| normalizer | Python and PostgreSQL MUST produce provenance hash `d91b94f256f79c6bc7b29fafa41c7a608b90c17bd479b29a64be00fa538c49fb`, all 29 code-point vectors, nonmembers, no Unicode normalization, and exact chunk/requirement hashes. |
| kind/null table | Every legal job/completion shape MUST construct and every illegal pair/scope/parent/reason/result/closure combination MUST fail. |
| DTO identity | One-field mutation tests MUST change each channel-hit, selection, discovery, admitted-pair, pair-input, verifier, observation, attempt-output/result, activation, state-reference, work, and run-result digest. |
| dedup | Every permutation of overlapping forward/reverse/fallback root results MUST choose the same least root, child set, closures, admitted rows, and barrier hash. |
| frontier | Multi-chunk deletion MUST create at most one fallback per requirement/policy; empty/short exhaustion MUST close; unavailable/retry exhaustion MUST block; nonselected hits MUST never promote. |
| PENDING | Shared-owner reverse roots, multiple child pairs, required/optional owners, terminal failure, cancellation, late return, and empty closure MUST preserve exact multiplicity without negative counters. |
| late result | All eight truth combinations for epoch/subject/group/chunk activity MUST obey `EPOCH_FAILED > SUBJECT_INACTIVE > CHUNK_INACTIVE`; exact replay MUST be no-op and conflict MUST roll back. |
| compatibility | Frozen M4 pair/job/input/artifact/observation/publication golden vectors MUST remain byte-identical after importing M5 runtime modules. |

### 18.2 Deterministic fake-port history

`tests/m5/runtime/test_typed_history.py` MUST run one deterministic history
covering:

1. new requirement forward top-k plus inserted-chunk reverse discovery with an
   overlapping pair deduplicated before closure;
2. requirement SUPPORT creating a complete group without direct support;
3. requirement REFUTE and NEUTRAL creating no parent refutation;
4. alternative witness repair;
5. nonfinal matching-edge loss with no requirement zero crossing;
6. final assignment loss and surviving alternative group;
7. direct support surviving group loss and direct refutation conflicting with
   group support;
8. deletion fallback with empty success, short success, temporary
   unavailability, retry, and terminal failure;
9. optional-owner PENDING and multiple-owner multiplicity;
10. cancellation followed by a stale attempt after a later replacement;
11. failed-epoch late completion; and
12. reconnect replay with zero external calls.

Every successful event MUST produce the same full structured state and
certificates as the independent Python oracle. The fake ports MUST record exact
call order and MUST fail if model work occurs inside a transaction.

### 18.3 PostgreSQL schema and runtime tests

`tests/m5/postgres_runtime/test_migration_015.py` MUST prove:

- rejection without the accepted 014 ledger row;
- rejection on an incorrect 014 bundle hash;
- fresh 014-to-015 install, populated install, exact rerun, hash-conflict
  rejection, mid-DDL rollback, and deferred validation;
- no activation and no M4-v1 byte change from migration alone;
- exact `v1_only` guard behavior; activated public-v1 rejection with no
  event/epoch consumption; acceptance only after an exact typed document
  sidecar inserted by the current transaction; and rejection of wrong order,
  prior-committed sidecars, wrong kind, event, predecessor, candidate policy,
  manifest, decision policy, registry snapshot, revision, or runtime state;
- deferred rejection of a document sidecar without its matching M4 row and of
  a non-document sidecar with one, plus injected open rollback leaving neither
  declaration nor any child/PENDING row;
- rejection of public M4 resume, completion, failure, and seal on an epoch with
  a typed runtime header, with zero row/head change;
- every SQL enum/nullability/immutability/subtype/counter/closure constraint;
  and
- indexed point lookups for epoch, job, scope, attempt, pair, frontier,
  PENDING, and durable result.

`tests/m5/postgres_runtime/test_typed_runtime.py` MUST execute the fake-port
history through the real store and MUST compare every affected table after
exact replay and conflicting replay.

### 18.4 Race matrix

The live race suite MUST force both serial orders and assert one legal outcome
for each pair:

| Race | Required outcome |
|---|---|
| activation vs v1 open | v1 durable open first MUST make activation reject until terminal; activation first MUST make v1 open reject before event consumption. |
| activation vs typed open | typed open before active MUST reject; activation first MUST permit one head-equal typed open. |
| two typed opens | the single-open-epoch constraint MUST permit one and reject/queue the other without a second live epoch. |
| two acquisitions | one lease/attempt ordinal MUST win; loser MUST observe conflict/replay without model dispatch. |
| two root-result returns | both result rows MUST persist, but no closure MUST appear until the complete barrier; concurrent final result/barrier MUST close once. |
| cancellation vs verifier completion | the serialized winner MUST determine completion or cancellation; loser MUST become exact replay/conflict/audit-only without double decrement. |
| failure vs late return | failure MUST win publication; late return MUST be audit-only with no revision. |
| seal vs final completion | completion first MUST permit one seal; seal first MUST reject while work is open. |
| two seals | one publication transaction MUST commit; the other MUST exact-replay the same result. |

### 18.5 Crash and reconnect matrix

Failure injection MUST occur after each logical write group in structural open,
root-result staging, event-wide barrier, verifier archival, active currency,
matching/state patch, cancellation, durable failure, M4 promotion, M5
promotion, each head advance, event-result insert, and epoch seal. After a new
connection:

- every precommit injection MUST show the exact prior table projection;
- every postcommit injection MUST show the complete new projection;
- no orphan child, half closure, mismatched counter, one-head advance,
  duplicated observation, duplicated public delta, or partial run result MUST
  exist; and
- reconnect MUST reuse every committed discovery/verifier artifact and call
  only missing external work.

### 18.6 Three-oracle measured history

The measured kernel MUST monkeypatch Python full recomputation, SQL recursive
oracle entrypoints, pure runtime-book reconstruction, aggregate job scans, and
full repository hydration to raise. Each seal MUST still succeed using point
persisted invariants. Immediately after each measured seal, a separate audit
transaction MUST compare:

```text
incremental persisted state and certificates
  == independent Python full recomputation
  == independent SQL full recomputation
```

The audit MUST cover requirement, group, claim, answer, currency, certificate,
coordination, PENDING, and publication projections. A seeded divergence in
each projection MUST be detected. Oracle time MUST be excluded from measured
incremental latency and reported separately.

### 18.7 Pinned-model diagnostic

The opt-in diagnostic MUST pin model artifact tree hash/revision, tokenizer,
prompt, role templates, calibration, temperature, decision policy, candidate
policy, PostgreSQL/regconfig identity, both snapshots, exact pair inputs,
normalizer provenance, random seed/config, and hardware/software manifest. It
MUST enforce the frozen forward/reverse budgets and bounded event cap. It MUST
persist raw logits/scores/output hashes, tokens, component timing, work
counters, retries, failures, and all result identities. It MUST label the
result `diagnostic` and MUST NOT treat model output as semantic gold or tune a
threshold/prompt/model from the diagnostic result.

## 19. Honest runtime-work claim

M5.4 MUST report the direct M4 structured cost under the corrected M4.7 bound,
the M5 matching overlay under M5-T2, and the new runtime coordination work as
separate components. Runtime coordination MUST charge:

```text
exact withdrawal edge visits
+ sorting each root's channel hits and selections
+ sorting the event root/result/admitted sets
+ event-wide pre-dedup selections and unique admitted pairs
+ every job/attempt/closure/counter transition
+ every touched requirement/group/claim/answer/certificate row
+ bytes hashed and serialized
```

The logical structured bound MUST exclude embedding, lexical/vector retrieval,
neural verification, PostgreSQL B-tree factors, query planning, triggers,
locks, lock waiting, WAL, buffer/cache behavior, disk I/O, network, and external
artifact I/O. Those exclusions MUST appear as separate calls/tokens/timings and
physical counters in every report. `postgres_roundtrip_wall_ns` MUST NOT be
described as structured RAM work. Oracle/audit time MUST not enter measured
seal or incremental latency.

M5.4 MUST NOT claim worst-case sublinear updates, constant physical database
work, semantic retrieval completeness, cryptographic privacy, model truth, or
superiority to DBSP, F-IVM, CROWN, or another named system from these
contracts. Dense deletion, high fanout, large result bytes, model work, and
database I/O MUST remain explicit.

## 20. Contract-to-decision audit

| Frozen item | Runtime specialization that MUST preserve it |
|---|---|
| M5-D1 | Sections 3, 5, 7, and 8 use typed framing, exact normalizer provenance, immutable versions, NULL, F64, and canonical ordering. |
| M5-D2 | Sections 11 and 14 declare successor forward work atomically and cancel retired ownership without editing immutable versions. |
| M5-D3 | Sections 8.4 and 14.3 archive inactive/late observations as ineligible and prohibit currency transfer. |
| M5-D4 | Every pair/job/observation binds the typed subject registry and requirement subtype. |
| M5-D5 | Only current eligible canonical SUPPORT observations derive witness edges; no writable witness DTO/table is introduced. |
| M5-D6 | Verifier completion invokes exact distinct-hash matching/combined-state patches from migration 014/M5.2. |
| M5-D7 | Completion charges every net distinct edge crossing; fallback does not use requirement-satisfaction zero crossing as a shortcut. |
| M5-D8 | Runtime delegates bounded Hall-mask state to the accepted matching kernel and never recomputes it from a hidden reserve. |
| M5-D9 | Seal validates and publishes policy/epoch/revision-bound group certificates and changed references atomically. |
| M5-D10 | Section 2 retains the exact direct M4-v1 subgraph and combines it only at typed seal. |
| M5-D11 | Section 7.3 makes requirement REFUTE/NEUTRAL inert for parent refutation. |
| M5-D12 | Sections 10 and 14 publish full v2 claim-certificate references even without a status delta. |
| M5-D13 | Sections 14.5 and 18.6 keep Python/SQL full recomputation independent and out of measured seal. |
| M5-D14 | Sections 4 through 8 retain all frozen v2 job/snapshot/scope/pair/completion recipes and total shapes. |
| M5-D15 | Section 13 defines owner/required-answer PENDING, exact multiplicity, lazy reverse roots, and atomic closure replacement. |
| M5-D16 | Sections 10, 18, and 19 persist event-level work/provenance and preserve empirical/exact separation. |
| M5-D17 | The runtime accepts controlled rows only through canonical requirement events/artifacts and does not redefine WiCE labels or cohorts. |
| M5-D18 | Section 19 composes rather than weakens M5-T2 and records every excluded physical/neural cost. |
| M5-D19 | `verify_requirement_v1` is the only task entering witness state. |
| M5-D20 | The pinned-model run remains diagnostic and cannot confirm semantics. |
| M5-D21 | Sections 2, 14.1, and 16 admit the exact M4-v1 direct declaration only behind a matching same-transaction typed sidecar, preserve activated public-v1 rejection, and make the outer typed coordinator final authority for combined epoch state. |
| M5-T1 | Section 18.6 requires incremental/Python/SQL equality after every relevant measured seal. |
| M5-T2 | Sections 10 and 19 expose touched rows, bytes, model calls, and physical exclusions without hiding them in the affected-group bound. |

The M4/M5 identity boundary MUST remain:

| Object | CLAIM direct subgraph | REQUIREMENT subgraph |
|---|---|---|
| pair | `PairKey` | `SemanticPairKey` + `m5-semantic-pair-v2` |
| job | `m4-logical-job-v1` | `m5-job-payload-v2` + `m5-logical-job-v2` |
| child set | `m4-child-set-v1` | `m5-child-set-v2` |
| completion | `m4-job-completion-v1` and `m4-expandable-completion-v1` | `m5-job-completion-v2` + scope closure |
| verifier input | `m4-verification-pair-input-v1` | `m5-requirement-pair-input-v2` |
| verifier artifact | `m4-pair-verification-artifact-v1` | `m5-requirement-verifier-result-v2` / artifact-v2 |
| observation | `m4-semantic-observation-v1`, task `verify` | `m5-requirement-semantic-observation-v2`, task `verify_requirement_v1` |
| publication | `m4-publication-v1` | same preserved receipt ID plus v2 changed-state/run-result sidecars |

## 21. Final decision

**Decision: GO. Confidence: high.**

Revision 2 resolves the runtime ambiguities under M5-D21 without changing the
M5.0 evidence-group semantics or any v1 identity. The only hard sequencing
dependency is the accepted 014 schema bundle and its exact
activation/head/core relations. M5.4 implementation MUST remain blocked until
that dependency is merged and validated; this sequencing block MUST NOT be
reported as a design NO-GO.
