# M5-D25 Persisted Matching Runtime Candidate

Status: remediated contract candidate for independent same-byte adversarial
review; not frozen and not implementation authority

Date: 2026-09-02

Dependency: the M5-D24 contract, its C1 through C7 corrections, and migration
`migrations/016_m5_runtime_recovery.sql` are accepted. The migration-016
schema/installer boundary is pinned under bundle ID
`m5-runtime-recovery-schema-bundle-v1`; its exact five-field ledger appears in
Section 3 and has no byte drift from acceptance commit `6187589`. Remaining
M5-D24 production seal/runtime evidence is still incomplete, but it is not a
license to guess D25 behavior. M5-D25 MUST NOT freeze, authorize
implementation, or assign migration-017 ownership until this complete
candidate has passed the independent exact-byte reviews required by Section
15 and its corresponding authority amendments have been accepted.

Authority boundary: this candidate proposes one new physical-runtime decision
under the already frozen M5-D7, M5-D8, M5-D9, M5-T1, and M5-T2 semantics. It
does not authorize implementation until the design freeze, runtime addendum,
acceptance matrix, implementation plan, and execution manifest all record an
accepted amendment.

## 1. Blocking production evidence

The frozen M5 design requires maintained positive edge refcounts, group/hash
masks, mask histograms, Hall neighbour counts and deficiencies, ordered hashes
per mask, and ordered active observation IDs per edge. The accepted Python
kernel maintains those values only inside `M5IncrementalOverlay` process
memory.

Migration 014 deliberately persists semantic working/materialized/published
requirement, group, claim, and answer projections and certificate artifacts,
but explicitly contains no Hall-mask implementation state. Migration 015 adds
runtime coordination, attempts, PENDING, work, and event-result relations, but
its exact relation list contains no physical matching state. Consequently a
production verifier completion after reconnect must currently do at least one
of the following forbidden things:

1. trust a lost process-local `M5IncrementalOverlay` cache;
2. hydrate the full repository and rebuild the in-memory overlay;
3. scan base relations or invoke a Python/SQL full oracle in the measured
   transition; or
4. derive a certificate representative without the frozen ordered mask/hash
   and edge/observation indexes.

None satisfies runtime-addendum revision 5 Sections 14.3, 14.5, 14.6, or 18.6
as specialized by the accepted M5-D24 and C1 through C7 corrections.
M5.4-05, M5.4-06, M5.4-07, and maintained M5.5 evidence therefore remain
blocked even after recoverable dispatch and durable work are corrected by
M5-D24.

## 2. Proposed M5-D25 decision

### M5-D25 -- durable incremental matching state

After M5 activation, every matching-affecting semantic microtransaction MUST
maintain PostgreSQL-resident current and epoch-working physical state for:

- the decision-policy identity and exact publication point of the current and
  epoch-working physical images;
- current active SUPPORT-observation membership and its exact edge
  provenance;
- positive `(requirement_version_id, text_hash)` edge refcounts;
- nonzero `(group_version_id, text_hash)` adjacency masks;
- exact bounded `C`, `N`, deficiency, maximum-deficiency, matching-size, and
  distinct-hash state per active group; and
- immutable byte-total physical/logical transition patches, exact per-source
  matching-work contributions, and exact accumulated M5 matching/overlay
  work.

The current physical relations MUST represent exactly the latest sealed M5
publication head. The one live typed epoch MUST use an epoch-keyed working
overlay over that current image. A successful typed seal MUST promote the
working overlay to current in the same transaction as semantic state,
certificate, currency, M4/M5 head, event-result, and epoch publication. A
terminal failure MUST retain its working physical rows as audit evidence but
MUST NOT promote them or change current, materialized, published, certificate,
or head state.

Reconnect MUST derive every next transition from committed PostgreSQL rows.
The production open, completion, reconnect, failure, and seal paths MUST NOT
instantiate `M5IncrementalOverlay` by any constructor, retain an overlay or
matching index across transactions, consult another process-local matching
cache, invoke either full-recomputation oracle, scan all jobs, or hydrate the
full repository. They may call the accepted pure bounded Hall and certificate
value functions over cursor-local point results inside one checked
transaction.

### 2.1 Non-change boundary

M5-D25 changes no semantic truth table, observation decision, group
completeness rule, certificate content recipe, M4-v1 identity, M5-D14 runtime
identity, M5-D22 changed-state identity, M5RuntimeWork digest, or
M5EventRunResult logical-result digest.

The new relations are derived physical implementation state. They MUST NOT be
read by the independent Python oracle or either independent SQL oracle. The
existing migration-014 relations remain authoritative for semantic working,
materialized, and published state and for immutable group/claim certificate
artifacts and bindings.

M5-D25 introduces non-semantic physical-image identities plus patch,
contribution, and work digests solely for transition induction, replay conflict
detection, and reproducible M5-T2 reporting. None may enter an event payload,
observation, job, attempt, completion, changed-state reference, publication
receipt, or logical-result identity. A D25 contribution is not a D24 work
contribution or timing anchor.

## 3. Exact migration and bundle identity

The migration path MUST be exactly:

```text
migrations/017_m5_persisted_matching.sql
```

The accepted migration-016 prerequisite is recorded as these five checked
literal fields:

```text
accepted_016_bundle_id = "m5-runtime-recovery-schema-bundle-v1"
accepted_016_migration_sha256 = a63d2a878a5196e071e3e51c6e6737cf76552057ade65da4112e0f0bafb412d7
accepted_016_bundle_sha256 = 28a31f37c13cdaa2b89676e6279740a1f366e1acd16502c4fa722c2e0be21565
accepted_016_oracle_sha256 = e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855
accepted_016_prerequisite_sha256 = b7b03574dc2ba62fd6ba7be22744e2fe6d9ec178ffb2b4b9b552c5ff6281dacd
```

All four digests are 64-character lowercase literals independently recomputed
from or compared with the accepted bytes. Before freeze, the bundle ID and all
four digests MUST be rechecked on the exact candidate base rather than accepted
as whatever values happen to exist under one ID. No placeholder,
computed-at-runtime accepted value, wildcard, or "current ledger value"
wording is permitted in an authoritative M5-D25 contract.

Let:

```text
migration_017_sha256 = sha256(exact bytes of
  migrations/017_m5_persisted_matching.sql)
```

The new bundle digest MUST be:

```text
stable_m5_digest(
  "m5-persisted-matching-schema-bundle-v1",
  *TEXT("migrations/017_m5_persisted_matching.sql"),
  *HASH(migration_017_sha256),
  *HASH(accepted_016_bundle_sha256))
```

Before any migration-017 DDL, the installer MUST require one exact ledger row
matching all five accepted-016 literals. The installer MUST atomically record:

```text
bundle_id = "m5-persisted-matching-schema-bundle-v1"
bundle_sha256 = the digest above
migration_sha256 = migration_017_sha256
oracle_sha256 = e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855
prerequisite_sha256 = accepted_016_bundle_sha256
```

The empty `oracle_sha256` is required because migration 017 adds no oracle
member. On every invocation the installer MUST inspect the migration-017
ledger ID first: an exact already-installed row is a no-op even if typed
history now exists; the same ID with different content conflicts. Only a
first installation proceeds to accepted-016 validation, locks, no-history
checks, DDL, and backfill. Missing, mismatched, placeholder, or unvalidated
migration-016 evidence MUST abort before any 017 DDL. Any injected failure
before commit MUST leave neither partial DDL nor a ledger row.

## 4. Exact persisted relation families

Every text ordering below means ascending UTF-8 byte order with PostgreSQL
`COLLATE "C"`. Every SHA-256 field is exactly 64 lowercase hexadecimal
characters. Every revision is nonnegative, and every runtime epoch/revision
write is checked against the already-held expected-revision CAS.

### 4.1 Current and working physical-image identity

Policy identity belongs to the physical image, not to each unchanged Hall row.
This avoids rewriting every active group for a zero-flip policy change while
still proving which policy-relative witness graph the physical rows represent.

```text
groundloop_m5_matching_image_current(
  singleton boolean PRIMARY KEY CHECK(singleton),
  decision_policy_version text NOT NULL,
  installed_epoch_id bigint NOT NULL,
  installed_revision bigint NOT NULL
)
```

In `v1_only` before activation this relation is empty. In `m5_active` it has
exactly one row. Its `installed_epoch_id` MUST equal both the M4 publication
head's `epoch_id` and the M5 publication head's `epoch_id`; its
`installed_revision` MUST equal the M5 head's `sealed_revision` and that sealed
epoch's durable revision. The M4 head has no revision field and MUST NOT be
treated as if it did.

```text
groundloop_m5_matching_image_working(
  epoch_id bigint PRIMARY KEY,
  base_epoch_id bigint NOT NULL,
  base_revision bigint NOT NULL,
  decision_policy_version text NOT NULL,
  updated_revision bigint NOT NULL
)
```

Structural open inserts exactly one working-image row. Its base point MUST
equal the locked current-image point: `base_epoch_id` equals both predecessor
head epoch IDs, while `base_revision` equals only the M5 predecessor
`sealed_revision` and predecessor epoch revision. Its policy MUST equal
`groundloop_m5_update.decision_policy_version`, the typed candidate policy's
decision-policy version, and the direct candidate policy when a direct
subgraph exists. Policy is immutable for that epoch. `updated_revision` is the
latest committed D25 semantic-transition revision, not necessarily the latest
runtime coordination revision.

Every effective read MUST first validate both image rows under the already-held
runtime-epoch lock. Seal updates the current image to the sealing point and
working policy in the same transaction as all other promotion. Failure retains
the working image and leaves current unchanged.

### 4.2 Current and working observation membership

```text
groundloop_m5_matching_observation_current(
  observation_id text PRIMARY KEY,
  requirement_version_id text NOT NULL,
  group_version_id text NOT NULL,
  requirement_ordinal integer NOT NULL,
  text_hash char(64) NOT NULL,
  installed_epoch_id bigint NOT NULL,
  installed_revision bigint NOT NULL
)
```

Each row means that the observation is in the latest sealed active SUPPORT
edge. It MUST reference the immutable semantic observation and exact
requirement/group/ordinal coordinates. The observation MUST be eligible for
currency, have `subject_kind='requirement'`, have
`task_type='verify_requirement_v1'`, and realize `text_hash` from the exact
M5-normalized chunk text.

```text
groundloop_m5_matching_observation_working(
  epoch_id bigint NOT NULL,
  observation_id text NOT NULL,
  requirement_version_id text NOT NULL,
  group_version_id text NOT NULL,
  requirement_ordinal integer NOT NULL,
  text_hash char(64) NOT NULL,
  present boolean NOT NULL,
  updated_revision bigint NOT NULL,
  PRIMARY KEY(epoch_id, observation_id)
)
```

`present=false` is an explicit tombstone. One observation may move
`present=true -> false -> true` inside one epoch, but its requirement, group,
ordinal, and hash coordinates MUST never change. Working rows MUST never be
deleted.

Required representative indexes are:

```text
current(group_version_id, requirement_ordinal,
        text_hash COLLATE "C", observation_id COLLATE "C")
working(epoch_id, group_version_id, requirement_ordinal,
        text_hash COLLATE "C", observation_id COLLATE "C")
  WHERE present
```

### 4.3 Current and working edge refcounts

```text
groundloop_m5_matching_edge_current(
  requirement_version_id text NOT NULL,
  text_hash char(64) NOT NULL,
  group_version_id text NOT NULL,
  requirement_ordinal integer NOT NULL,
  refcount bigint NOT NULL,
  installed_epoch_id bigint NOT NULL,
  installed_revision bigint NOT NULL,
  PRIMARY KEY(requirement_version_id, text_hash)
)
```

Current `refcount` MUST be strictly positive.

```text
groundloop_m5_matching_edge_working(
  epoch_id bigint NOT NULL,
  requirement_version_id text NOT NULL,
  text_hash char(64) NOT NULL,
  group_version_id text NOT NULL,
  requirement_ordinal integer NOT NULL,
  refcount bigint NOT NULL,
  updated_revision bigint NOT NULL,
  PRIMARY KEY(epoch_id, requirement_version_id, text_hash)
)
```

Working `refcount` MUST be nonnegative. Zero is an explicit tombstone. Group
and ordinal coordinates are immutable for the key. Working rows MUST never be
deleted.

### 4.4 Current and working group/hash masks

```text
groundloop_m5_matching_hash_mask_current(
  group_version_id text NOT NULL,
  text_hash char(64) NOT NULL,
  mask integer NOT NULL,
  installed_epoch_id bigint NOT NULL,
  installed_revision bigint NOT NULL,
  PRIMARY KEY(group_version_id, text_hash)
)
```

Current `mask` MUST be in `1..(2^r_g-1)`.

```text
groundloop_m5_matching_hash_mask_working(
  epoch_id bigint NOT NULL,
  group_version_id text NOT NULL,
  text_hash char(64) NOT NULL,
  mask integer NOT NULL,
  updated_revision bigint NOT NULL,
  PRIMARY KEY(epoch_id, group_version_id, text_hash)
)
```

Working `mask` MUST be in `0..(2^r_g-1)`. Zero is an explicit tombstone.
Working rows MUST never be deleted.

Required representative indexes are:

```text
current(group_version_id, mask, text_hash COLLATE "C")
working(epoch_id, group_version_id, mask, text_hash COLLATE "C")
  WHERE mask > 0
```

### 4.5 Current and working bounded Hall state

```text
groundloop_m5_matching_hall_current(
  group_version_id text PRIMARY KEY,
  requirement_count integer NOT NULL,
  mask_histogram bigint[] NOT NULL,
  neighbor_counts bigint[] NOT NULL,
  deficiencies bigint[] NOT NULL,
  maximum_deficiency integer NOT NULL,
  matching_size integer NOT NULL,
  distinct_hash_count bigint NOT NULL,
  installed_epoch_id bigint NOT NULL,
  installed_revision bigint NOT NULL
)
```

```text
groundloop_m5_matching_hall_working(
  epoch_id bigint NOT NULL,
  group_version_id text NOT NULL,
  present boolean NOT NULL,
  requirement_count integer,
  mask_histogram bigint[],
  neighbor_counts bigint[],
  deficiencies bigint[],
  maximum_deficiency integer,
  matching_size integer,
  distinct_hash_count bigint,
  updated_revision bigint NOT NULL,
  PRIMARY KEY(epoch_id, group_version_id)
)
```

`present=false` is the group-state tombstone and requires every Hall payload
column to be NULL. `present=true` requires every payload column. Current rows
are always present. Requirement count MUST be in `1..8`; every array MUST have
exactly `2^requirement_count` entries. Python tuple index zero maps to
PostgreSQL array subscript one. Therefore:

```text
mask_histogram[1] = 0
neighbor_counts[1] = 0
deficiencies[1] = 0
```

All histogram and neighbour entries MUST be nonnegative. For each nonempty
subset integer `S`, stored at SQL subscript `S+1`:

```text
deficiencies[S+1] = popcount(S) - neighbor_counts[S+1]
maximum_deficiency = max(0, max(deficiencies[2:]))
matching_size = requirement_count - maximum_deficiency
distinct_hash_count = sum(mask_histogram)
```

The checked row validator MUST also recompute each neighbour count from the
histogram through the bounded complement subset-zeta relation. This is a
local `O(r_g 2^r_g)` validator over at most 256 entries, not a relational
full-recomputation oracle.

### 4.6 Durable matching-work accumulator

```text
groundloop_m5_matching_work_accumulator(
  epoch_id bigint PRIMARY KEY,
  contribution_additions bigint NOT NULL,
  contribution_removals bigint NOT NULL,
  requirement_observation_changes_processed bigint NOT NULL,
  policy_candidate_observations bigint NOT NULL,
  ordered_policy_range_probes bigint NOT NULL,
  ordered_index_operations bigint NOT NULL,
  canonical_sort_items bigint NOT NULL,
  edge_refcount_keys_updated bigint NOT NULL,
  distinct_edge_crossings bigint NOT NULL,
  hash_mask_transitions bigint NOT NULL,
  hash_masks_initialized bigint NOT NULL,
  hall_zeta_additions bigint NOT NULL,
  hall_subset_entries_examined bigint NOT NULL,
  hall_neighbor_entries_changed bigint NOT NULL,
  hall_deficiency_entries_examined bigint NOT NULL,
  certificate_repairs bigint NOT NULL,
  certificate_reconstructions bigint NOT NULL,
  policy_rebindings bigint NOT NULL,
  representative_hashes_read bigint NOT NULL,
  representative_observations_read bigint NOT NULL,
  augmenting_searches bigint NOT NULL,
  augmenting_requirement_visits bigint NOT NULL,
  augmenting_edge_visits bigint NOT NULL,
  certificate_digest_input_bytes bigint NOT NULL,
  group_local_state_operations bigint NOT NULL,
  groups_touched bigint NOT NULL,
  claims_touched bigint NOT NULL,
  answers_touched bigint NOT NULL,
  claim_status_changes bigint NOT NULL,
  answer_status_changes bigint NOT NULL,
  output_bytes bigint NOT NULL,
  requirement_state_only_changes bigint NOT NULL,
  group_state_only_changes bigint NOT NULL,
  claim_state_only_changes bigint NOT NULL,
  group_certificate_only_changes bigint NOT NULL,
  claim_certificate_only_changes bigint NOT NULL,
  public_status_deltas bigint NOT NULL,
  matching_work_digest char(64) NOT NULL,
  updated_revision bigint NOT NULL
)
```

Every counter MUST be nonnegative. The digest MUST encode every integer above
in the exact displayed order, excluding `epoch_id`, `matching_work_digest`,
and `updated_revision`:

```text
stable_m5_digest(
  "m5-matching-work-v1",
  *INT(contribution_additions),
  *INT(contribution_removals),
  *INT(requirement_observation_changes_processed),
  *INT(policy_candidate_observations),
  *INT(ordered_policy_range_probes),
  *INT(ordered_index_operations),
  *INT(canonical_sort_items),
  *INT(edge_refcount_keys_updated),
  *INT(distinct_edge_crossings),
  *INT(hash_mask_transitions),
  *INT(hash_masks_initialized),
  *INT(hall_zeta_additions),
  *INT(hall_subset_entries_examined),
  *INT(hall_neighbor_entries_changed),
  *INT(hall_deficiency_entries_examined),
  *INT(certificate_repairs),
  *INT(certificate_reconstructions),
  *INT(policy_rebindings),
  *INT(representative_hashes_read),
  *INT(representative_observations_read),
  *INT(augmenting_searches),
  *INT(augmenting_requirement_visits),
  *INT(augmenting_edge_visits),
  *INT(certificate_digest_input_bytes),
  *INT(group_local_state_operations),
  *INT(groups_touched),
  *INT(claims_touched),
  *INT(answers_touched),
  *INT(claim_status_changes),
  *INT(answer_status_changes),
  *INT(output_bytes),
  *INT(requirement_state_only_changes),
  *INT(group_state_only_changes),
  *INT(claim_state_only_changes),
  *INT(group_certificate_only_changes),
  *INT(claim_certificate_only_changes),
  *INT(public_status_deltas))
```

Structural open creates this row at revision 1 with the exact structural
matching work. A patch with no physical, logical-state, artifact, binding, or
status-delta change still has empty change/binding sequences and zero
applicable operation counters, but it is not the all-zero work vector: its
mandatory accepted `m5-overlay-logical-output-v2` empty-record image is 71
bytes, has SHA-256
`b4e641b66a06cb7d204377c37cfe031d958ce6d959832620fc2e9441339581c3`,
and therefore contributes `output_bytes=71`. Every later active semantic
transition routed through the joint matching/combined-state path adds its
nonnegative work exactly once and records that transition's resulting
revision. Coordination-only acquisition, root staging/barrier, cancellation,
inactive completion, audit-only return, failure, and seal revisions do not
advance this D25 revision. Exact replay and conflict rejection change no
counter. A terminal epoch makes the row immutable; a separate terminal copy is
unnecessary because the epoch-keyed accumulator is retained.

### 4.7 Immutable patch artifacts and work contributions

The exact D25 source kinds are:

```text
structural_open
requirement_completion
direct_transition
```

All group register/replace/retire, document insert/delete/replace, standalone
claim `ObserveEvent`, `ObserveRequirementEvent`, and policy effects belong to
the one revision-1 `structural_open` source. Its source ID is the structural
event ID and its identity hash is the event payload hash, including for the
canonical rootless requirement-observation event. A standalone claim
`ObserveEvent` coalesces its direct-M4 observation/currency/state/certificate
effects and combined-v2 effects into that one contribution; it MUST NOT create
a second revision-1 `direct_transition`. Only an active requirement verifier
completion uses `requirement_completion`. Only a later cursor-local active M4
transition that enters the combined M5 overlay uses `direct_transition`.
Activation bootstrap has no source or contribution because it is outside a
typed event. Inactive and audit-only completions have no D25 patch or
contribution.

The immutable patch is:

```text
M5PersistedMatchingPatch(
  source_kind,
  source_id,
  source_identity_hash,
  before_epoch_id,
  before_revision,
  resulting_epoch_id,
  resulting_revision,
  decision_policy_version,
  group_shape_set_digest,
  observation_change_digests,
  edge_change_digests,
  mask_change_digests,
  hall_change_digests,
  logical_overlay_patch_digest,
  matching_work_digest,
  patch_digest
)
```

For structural open, the before point is the sealed predecessor and the
resulting point is `(epoch_id, 1)`. For every later source both points use the
same epoch and `resulting_revision=before_revision+1`. `source_id` is the
structural event ID, M5 attempt ID, or frozen M4 transition ID respectively.
Its identity hash is the event payload hash, attempt-result-artifact hash, or
frozen M4 transition digest respectively.

The group-shape set contains every group named by any physical or logical
change. It is sorted by group ID and each entry contains group ID,
requirement count, and the dense `(ordinal, requirement_version_id)` sequence:

```text
group_shape_set_digest = stable_m5_digest(
  "m5-persisted-matching-group-shape-set-v1",
  *SEQ(each group encoded as
       SEQ((TEXT(group_version_id), INT(requirement_count),
            SEQ(each requirement encoded as
                SEQ((INT(ordinal), TEXT(requirement_version_id)))
                in ordinal order)))
       in group-ID order))
```

Each physical change digest uses one exact point algebra. The enum values are
the literal lowercase strings `current` and `working`. `SEQ` preserves the
displayed order. Every Hall array uses numeric subset order `0..2^r_g-1`, not
PostgreSQL's one-based subscript. The exact point preimages are:

```text
OBSERVATION_CURRENT(row) =
  SEQ((ENUM(current), TEXT(observation_id),
       TEXT(requirement_version_id), TEXT(group_version_id),
       INT(requirement_ordinal), HASH(text_hash),
       INT(installed_epoch_id), INT(installed_revision)))

OBSERVATION_WORKING(row) =
  SEQ((ENUM(working), INT(epoch_id), TEXT(observation_id),
       TEXT(requirement_version_id), TEXT(group_version_id),
       INT(requirement_ordinal), HASH(text_hash), BOOL(present),
       INT(updated_revision)))

EDGE_CURRENT(row) =
  SEQ((ENUM(current), TEXT(requirement_version_id), HASH(text_hash),
       TEXT(group_version_id), INT(requirement_ordinal), INT(refcount),
       INT(installed_epoch_id), INT(installed_revision)))

EDGE_WORKING(row) =
  SEQ((ENUM(working), INT(epoch_id), TEXT(requirement_version_id),
       HASH(text_hash), TEXT(group_version_id), INT(requirement_ordinal),
       INT(refcount), INT(updated_revision)))

MASK_CURRENT(row) =
  SEQ((ENUM(current), TEXT(group_version_id), HASH(text_hash), INT(mask),
       INT(installed_epoch_id), INT(installed_revision)))

MASK_WORKING(row) =
  SEQ((ENUM(working), INT(epoch_id), TEXT(group_version_id),
       HASH(text_hash), INT(mask), INT(updated_revision)))

HALL_CURRENT(row) =
  SEQ((ENUM(current), TEXT(group_version_id), INT(requirement_count),
       SEQ(INT(value) for value in mask_histogram),
       SEQ(INT(value) for value in neighbor_counts),
       SEQ(INT(value) for value in deficiencies),
       INT(maximum_deficiency), INT(matching_size),
       INT(distinct_hash_count), INT(installed_epoch_id),
       INT(installed_revision)))

HALL_WORKING(row) =
  SEQ((ENUM(working), INT(epoch_id), TEXT(group_version_id), BOOL(present),
       OPTION(INT(requirement_count)),
       OPTION(SEQ(INT(value) for value in mask_histogram)),
       OPTION(SEQ(INT(value) for value in neighbor_counts)),
       OPTION(SEQ(INT(value) for value in deficiencies)),
       OPTION(INT(maximum_deficiency)), OPTION(INT(matching_size)),
       OPTION(INT(distinct_hash_count)), INT(updated_revision)))
```

For a present working Hall row every payload `OPTION` is present; for a Hall
tombstone every payload `OPTION` is `None`. No other nullable shape is valid.
Observation, edge, and mask tombstones remain present working-point encodings
with `present=false`, `refcount=0`, and `mask=0` respectively. A missing row is
different: it is `OPTION(None)` at the change level and has no point preimage.

The exact change preimages and digests are:

```text
observation_change_digest = stable_m5_digest(
  "m5-persisted-matching-observation-change-v1",
  *TEXT(observation_id), *OPTION(OBSERVATION_POINT(before)),
  *OPTION(OBSERVATION_POINT(after)))

edge_change_digest = stable_m5_digest(
  "m5-persisted-matching-edge-change-v1",
  *TEXT(requirement_version_id), *HASH(text_hash),
  *OPTION(EDGE_POINT(before)), *OPTION(EDGE_POINT(after)))

mask_change_digest = stable_m5_digest(
  "m5-persisted-matching-mask-change-v1",
  *TEXT(group_version_id), *HASH(text_hash),
  *OPTION(MASK_POINT(before)), *OPTION(MASK_POINT(after)))

hall_change_digest = stable_m5_digest(
  "m5-persisted-matching-hall-change-v1",
  *TEXT(group_version_id), *OPTION(HALL_POINT(before)),
  *OPTION(HALL_POINT(after)))
```

Every stored `*_change_preimage` is the exact byte stream consumed by
`stable_m5_digest`: the literal domain and then the flattened M5-D1 typed
field expansion above, with each expanded UTF-8 field preceded by its unsigned
eight-byte big-endian length. Its paired digest is SHA-256 of exactly those
bytes. PostgreSQL array binary, JSON, `repr`, delimiter joining, ambient NULL
encoding, or later reserialization is forbidden. At least one of before/after
MUST be present.

`*_POINT` selects exactly the current or working recipe matching the encoded
layer tag; it is not a caller-defined union. The decoder MUST reject a point
whose repeated key differs from the outer key, whose layer tag and field shape
disagree, whose array cardinality differs from `2^requirement_count`, or whose
typed preimage has trailing or omitted fields.

Observation changes sort by observation ID. Edge changes sort by
`(group_version_id, requirement_ordinal, text_hash,
requirement_version_id)` after decoding and validating the repeated immutable
coordinates. Mask changes sort by `(group_version_id, text_hash)`, and Hall
changes by group ID. Every digest/preimage sequence is sorted in that same
order and unique by its outer physical key.

Logical state changes use the exact M5-D22 requirement/group/claim/answer
artifact hashes for present before/after rows and `OPTION(None)` for absence.
The `kind` enum is exactly `requirement_state | group_state | claim_state |
answer_state | group_certificate | claim_certificate`; certificate artifact
changes use the immutable certificate digest. Changes sort by
`(kind.value, object_id)`. Every working certificate-binding row is hashed as:

```text
stable_m5_digest(
  "m5-persisted-certificate-binding-row-v1",
  *ENUM(group|claim), *INT(epoch_id), *TEXT(object_id),
  *INT(valid_from_revision), *OPTION(INT(valid_to_revision)),
  *HASH(certificate_digest))
```

The exact binding sequence is not independently sorted. It is the decoded
`group_binding` block of `logical_output_preimage`, followed by that image's
decoded `claim_binding` block, preserving the already frozen producer/
first-touch object order. For one `(kind,object_id)` a patch contains at most
one close row and at most one open row, with the close immediately before the
open. A close has non-NULL `valid_to_revision=resulting_revision`, retains its
earlier `valid_from_revision`, and names the prior certificate. An open has
`valid_from_revision=resulting_revision`, NULL `valid_to_revision`, and names
the after certificate. Binding kind `group` maps only to output kind
`group_binding`, whose `object_id` equals the decoded binding's
`group_version_id`; binding kind `claim` maps only to output kind
`claim_binding`, whose `object_id` equals the decoded binding's `claim_id`.
The digest sequence and the two output binding blocks have one-to-one
equality; a missing, extra, duplicated, reordered, cross-object, or revision-
invalid row is rejected.

The byte-total logical patch is:

```text
logical_overlay_patch_digest = stable_m5_digest(
  "m5-persisted-logical-overlay-patch-v1",
  *SEQ(each sorted state/artifact change encoded as
       SEQ((ENUM(kind), TEXT(object_id),
            OPTION(HASH(before_state_or_artifact_hash)),
            OPTION(HASH(after_state_or_artifact_hash))))),
  *SEQ(HASH(binding_row_digest) for every ordered binding row),
  *HASH(logical_output_digest), *INT(output_bytes))
```

The referenced `m5-overlay-logical-output-v2` bytes remain the already accepted
in-memory output identity; D25 does not replace them with the sorted logical-
patch order above. Freeze the existing recursive encoder as follows. `FRAME(x)`
is the unsigned eight-byte big-endian length of `x` followed by `x`:

```text
LOGICAL(None)       = BYTE("n")
LOGICAL(false)      = BYTE("b") || BYTE(0)
LOGICAL(true)       = BYTE("b") || BYTE(1)
LOGICAL(Enum(v))    = BYTE("e") || FRAME(UTF8(v.value))
LOGICAL(str(v))     = BYTE("s") || FRAME(UTF8(v))
LOGICAL(int(v))     = BYTE("i") || FRAME(ASCII(base10(v)))
LOGICAL(float(v))   = BYTE("f") || IEEE754_BINARY64_BIG_ENDIAN(v)
LOGICAL(tuple(vs))  = BYTE("q") || UINT64_BE(len(vs))
                      || each FRAME(LOGICAL(v)) in tuple order
LOGICAL(dataclass)  = BYTE("d") || FRAME(UTF8(WIRE_TYPE_NAME))
                      || UINT64_BE(field_count)
                      || for each field in declaration order:
                           FRAME(UTF8(field.name))
                           || FRAME(LOGICAL(field.value))

logical_output_preimage = LOGICAL((
  "m5-overlay-logical-output-v2", tuple(output_records)))
logical_output_digest = sha256(logical_output_preimage)
output_bytes = len(logical_output_preimage)
```

Type dispatch occurs in the displayed order, so booleans are not encoded as
integers and enum values are not encoded as strings. Unsupported types,
non-tuple sequences, ambient serializers, normalization, and implicit
stringification are forbidden.

The dataclass wire allowlist is frozen below. The left value is the exact
`WIRE_TYPE_NAME`; the right tuple is the exact field name and value order:

```text
RequirementState =
  (requirement_version_id, witness_hashes, supporting_observation_ids,
   witness_count, satisfied)
GroupState =
  (group_version_id, requirement_count, satisfied_count, matching_size,
   complete)
CombinedClaimState =
  (claim_id, support_count, refute_count, best_support_score,
   best_refute_score, supporting_observation_ids,
   refuting_observation_ids, complete_group_count, complete_group_ids, status)
CombinedAnswerState =
  (answer_version_id, required_claim_count, supported_count,
   unsupported_count, refuted_count, conflicted_count, status)
GroupCertificateRow =
  (requirement_ordinal, requirement_version_id, text_hash,
   selected_observation_id)
GroupMatchingCertificateArtifact =
  (decision_policy_version, group_version_id, rows, certificate_version,
   certificate_digest)
ClaimCertificateArtifact =
  (claim_id, decision_policy_version, support_kind,
   direct_support_observation_id, group_version_id,
   group_certificate_digest, direct_refute_observation_id,
   certificate_version, certificate_digest)
WorkingGroupCertificateBinding =
  (epoch_id, group_version_id, valid_from_revision, valid_to_revision,
   certificate_digest)
WorkingClaimCertificateBinding =
  (epoch_id, claim_id, valid_from_revision, valid_to_revision,
   certificate_digest)
StatusDelta =
  (event_id, object_type, object_id, old_status, new_status, reason)
```

An implementation may use Python `__qualname__` or dataclass reflection only
after proving exact equality with this allowlist; ambient names or field order
never define the wire contract. Every float MUST be finite; negative zero is
preserved by the binary64 bits. The decoder MUST reject an unknown tag or wire
type, duplicate/missing/reordered/unknown field, noncanonical integer, invalid
or nonfinite binary64, length overrun, trailing byte, and any value whose exact
re-encoding differs from the stored preimage.

`output_records` retains the existing producer order, without a local sort.
The exact built-in `str` kind literals and block order are:

```text
requirement_state
group_state
claim_state
answer_state
group_certificate
claim_certificate
group_binding
claim_binding
status_delta
```

Hyphens, enum instances, aliases, or inferred class names are forbidden.
Within each block the producer preserves first-touch key order. Binding rows
use the exact sequence and close/open laws above. Each record is the
three-tuple `(kind, object_id, after_value)`, including an explicit `None`
after-value where applicable. The separately sorted logical state/artifact
change portion MUST decode to the same multiset, and the binding portion MUST
equal the two decoded output binding blocks one-for-one; neither may change
these accepted output bytes. Thus an empty physical patch can still bind
direct, state-only, certificate-only, or policy-rebind work.

The outer digest is:

```text
patch_digest = stable_m5_digest(
  "m5-persisted-matching-patch-v1",
  *ENUM(source_kind), *TEXT(source_id), *HASH(source_identity_hash),
  *INT(before_epoch_id), *INT(before_revision),
  *INT(resulting_epoch_id), *INT(resulting_revision),
  *TEXT(decision_policy_version), *HASH(group_shape_set_digest),
  *SEQ(HASH(value) for value in observation_change_digests),
  *SEQ(HASH(value) for value in edge_change_digests),
  *SEQ(HASH(value) for value in mask_change_digests),
  *SEQ(HASH(value) for value in hall_change_digests),
  *HASH(logical_overlay_patch_digest), *HASH(matching_work_digest))
```

Migration 017 stores the exact length-framed digest preimage, not merely a
caller-supplied hash:

```text
groundloop_m5_matching_patch_artifact(
  patch_digest char(64) PRIMARY KEY,
  source_kind text NOT NULL,
  source_id text NOT NULL,
  source_identity_hash char(64) NOT NULL,
  before_epoch_id bigint NOT NULL,
  before_revision bigint NOT NULL,
  resulting_epoch_id bigint NOT NULL,
  resulting_revision bigint NOT NULL,
  decision_policy_version text NOT NULL,
  group_shape_set_digest char(64) NOT NULL,
  group_shape_set_preimage bytea NOT NULL,
  observation_change_digests char(64)[] NOT NULL,
  observation_change_preimages bytea[] NOT NULL,
  edge_change_digests char(64)[] NOT NULL,
  edge_change_preimages bytea[] NOT NULL,
  mask_change_digests char(64)[] NOT NULL,
  mask_change_preimages bytea[] NOT NULL,
  hall_change_digests char(64)[] NOT NULL,
  hall_change_preimages bytea[] NOT NULL,
  logical_overlay_patch_digest char(64) NOT NULL,
  logical_overlay_patch_preimage bytea NOT NULL,
  logical_output_preimage bytea NOT NULL,
  matching_work_digest char(64) NOT NULL,
  canonical_patch_preimage bytea NOT NULL
)
```

The checked decoder MUST prove that each preimage array has the same
cardinality and order as its paired digest array, each child preimage has the
exact typed shape above and hashes to the paired child digest, and the retained
group-shape preimage hashes to `group_shape_set_digest`. It MUST decode the
logical preimage into every ordered state/artifact change and binding row,
prove that `logical_output_preimage` is the exact accepted
`m5-overlay-logical-output-v2` byte image, and recompute its SHA-256 digest and
`output_bytes`. Finally it MUST recompute `logical_overlay_patch_digest` and
decode the outer preimage to recompute `patch_digest`. No omitted child bytes
may be recovered from mutable final working rows. The row is immutable and
retained after seal or failure.

```text
groundloop_m5_matching_work_contribution(
  epoch_id bigint NOT NULL,
  source_kind text NOT NULL,
  source_id text NOT NULL,
  source_identity_hash char(64) NOT NULL,
  before_epoch_id bigint NOT NULL,
  before_revision bigint NOT NULL,
  resulting_revision bigint NOT NULL,
  patch_digest char(64) NOT NULL,
  every Section-4.6 counter in exact displayed order,
  matching_work_digest char(64) NOT NULL,
  contribution_digest char(64) NOT NULL,
  PRIMARY KEY(epoch_id, source_kind, source_id),
  UNIQUE(epoch_id, resulting_revision)
)
```

```text
contribution_digest = stable_m5_digest(
  "m5-matching-work-contribution-v1",
  *INT(epoch_id), *ENUM(source_kind), *TEXT(source_id),
  *HASH(source_identity_hash), *INT(before_epoch_id),
  *INT(before_revision), *INT(resulting_revision),
  *HASH(patch_digest), *HASH(matching_work_digest))
```

The store computes the patch and work from locked PostgreSQL points, actual
ordered probes/representative reads, and the jointly validated logical patch.
External workers do not supply either. First application atomically inserts
the patch artifact and contribution, updates the working rows, logical rows,
accumulator, working-image revision, job/PENDING state, and runtime revision.
Exact replay requires every source, point, patch, and work identity to match
and performs zero writes. Reuse of a source key or resulting revision with any
different field conflicts and rolls back. The accumulator is updated only by
the first contribution insert.

A deferred checked validator rooted on the runtime revision, working image,
every D25 working relation, migration-014 semantic/certificate rows, patch
artifact, and contribution MUST enforce the one-transition bijection. No D25
physical/logical row may change at a semantic revision without exactly one
matching contribution, and a contribution may not exist without its complete
joint after image. Coordination-only revisions are explicitly outside that
bijection. Comments or application convention are not enforcement.

## 5. Effective current-plus-working semantics

Before resolving any physical key, the store MUST lock and validate the current
and working image headers from Section 4.1. The current image epoch equals both
publication-head epoch IDs, its revision equals the M5 `sealed_revision` and
sealed epoch revision, the working base equals that full current image point,
and the working policy equals the typed update/candidate policy.

For one active epoch and one physical key:

1. take its working row for that epoch if present;
2. otherwise take the current row;
3. filter a working observation on `present`, an edge on `refcount > 0`, a mask
   on `mask > 0`, and Hall state on `present`.

A current row is eligible only after an anti-join proves that **no** working
row with the same physical key exists. The anti-join occurs before filtering
working `present/refcount/mask` values. Therefore a working tombstone or a
working row that moved one hash to another mask shadows the old current row;
filtering positive working rows first and then unioning current is forbidden.

A working add followed by removal may leave a tombstone even when no current
row existed. A working removal followed by re-addition may end present. The
final working image, not the number of intermediate mutations, is what seal
promotes. Exact intermediate physical/logical patches remain in the immutable
Section-4.7 artifact/contribution ledger, while certificate history remains in
the existing revision-interval binding tables.

The typed runtime's expected predecessor MUST equal both current publication
heads before any effective read. The single-open-structural-epoch rule is a
required assumption; these overlays do not define concurrent branch merge
semantics.

At seal:

- the working image policy and sealing point replace the current image header;
- present observation rows upsert current; tombstones delete current;
- positive edge rows upsert current; zero rows delete current;
- positive mask rows upsert current; zero rows delete current;
- present Hall rows upsert current; Hall tombstones delete current; and
- every current upsert records the sealing epoch and revision.

Working rows, image rows, patch artifacts, contributions, and accumulators are
retained after both seal and failure. Unauthorized direct inserts, updates, or
deletes against any D25 current, working, image, patch, contribution, or
accumulator relation MUST be rejected by privileges and triggers; only the
checked cursor-local procedures and the separately authorized migration/
activation bootstrap may write them.

Migration 017 MUST install a `BEFORE INSERT OR UPDATE OR DELETE` guard trigger
on every D25 current, working, image, patch, contribution, and accumulator
relation. Runtime DML requires the existing transaction-local
`groundloop.m5_checked_transition=on` authorization set by the accepted
`groundloop_m5_authorize_checked_transition(epoch_id,expected_revision)` only
after its tier locks and CAS validation. That Boolean alone is insufficient:
migration 017 MUST also install
`groundloop_m5_authorize_persisted_matching_transition(
epoch_id bigint, expected_runtime_revision bigint, resulting_revision bigint,
source_kind text, source_id text)` and
`groundloop_m5_authorize_persisted_matching_seal(
epoch_id bigint, expected_revision bigint, sealed_revision bigint)`.
Each companion helper first requires the accepted Boolean authorization and
rechecks the already-held epoch/runtime rows. The transition helper validates
the Section-4.7 source/revision law plus the structural-open exception below;
the seal helper validates the frozen seal point. They then set transaction-
local D25 context fields with exact mode `transition | seal`, epoch ID,
expected-runtime/resulting revision, and, for a transition, source kind/ID.

For `structural_open`, the base and typed runtime rows for the new epoch have
already been inserted and locked at revision 1. The caller MUST invoke the
accepted authorization as `(epoch_id, 1)` and the D25 transition helper as
`(epoch_id, 1, 1, structural_open, structural_event_id)`. This runtime-
authorization point does not alter the patch: its before point remains the
sealed predecessor and its resulting point remains `(epoch_id, 1)`. For every
later transition from runtime revision `N`, both helpers receive expected
runtime revision `N` and the D25 helper receives resulting revision `N+1`.
Revision-zero authorization and using the predecessor revision as the new
epoch's expected runtime revision are forbidden.

Activation DML requires the new checked
`groundloop_m5_authorize_persisted_matching_activation(
expected_m4_head_epoch_id bigint, expected_head_epoch_revision bigint,
decision_policy_version text)` helper installed by 017. That helper locks and
validates mode, the M4 head and referenced epoch revision, M5-head/activation
absence, the exact 017 ledger, policy and zero-live-epoch preconditions before
setting the accepted Boolean plus D25 context mode `activation`, installed
epoch/revision and policy.

Every guard requires both the accepted Boolean and one complete D25 context.
Transition mode admits only the named epoch/source/revision's working image,
working physical rows, patch, contribution and accumulator operations. Seal
mode admits only the named promotion's current-image/current-row operations,
with exact installed epoch/revision. Activation mode admits only bootstrap
current-image/current-row operations at its checked head/policy. A wrong mode,
epoch, expected/resulting revision, source, relation family or operation is
rejected before DML; deferred validators still enforce the complete
transition/promotion bijection. First-install DDL/backfill creates and
validates its rows before enabling these guards, then installs every guard
before the ledger insert and commit. No ordinary application path may set any
authorization context directly or disable a guard. This is a database-
correctness boundary for trusted GroundLoop credentials, not a security claim
against a malicious schema owner.

## 6. Localized transition validators

The cursor-local store MUST validate all fallible before images before its
first physical write.

For each matching patch it MUST prove locally:

1. the image headers, source identity, before/resulting points, policy, group
   shapes, patch preimage, and contribution key/digest are exact;
2. every observation change has immutable requirement/group/ordinal/hash
   coordinates and names the exact typed observation;
3. every active membership is eligible, current at the working currency
   revision, canonical-task SUPPORT, and active under chunk/group validity;
4. signed observation changes coalesce to the store-derived edge-refcount
   changes;
5. no edge refcount underflows;
6. only `0 -> positive` and `positive -> 0` edge crossings change a mask bit;
7. all edge changes for one `(group,text_hash)` coalesce before its one
   old-mask/new-mask transition;
8. a net-equal mask performs zero Hall work;
9. every Hall after-row is exactly the frozen kernel result from its before-row
   and sorted unique mask transitions;
10. every working requirement state written by the patch equals its effective
    witness hashes/observation IDs and records the patch policy;
11. every working group state written by the patch has exact Hall scalar
    agreement and the patch policy; an inherited numerically unchanged D25
    Hall row is interpreted under the physical-image policy without forcing an
    all-group physical rewrite, but this header MUST NOT reinterpret or excuse
    any migration-014 semantic row whose policy field the frozen semantic
    patch requires to change;
12. every complete group has one valid open working certificate binding;
13. every affected claim/answer and certificate transition agrees with the
    existing frozen combined-state rules; and
14. all 37 logical counters, `logical_output_digest`, and `output_bytes` are
    recomputed from the joint physical/logical transition and actual indexed
    operations before the contribution is inserted.

These checks rely on transition induction from a validated sealed state and
the immutable patch ledger. They MUST NOT be weakened into an inline
base-relation scan, a caller-asserted work vector, an `M5IncrementalOverlay`
instance, or a process cache. A physical-empty direct or policy-rebind patch is
valid only when its exact logical patch and nonzero/zero work are still bound.
Independent full recomputation remains the post-seal audit.

### 6.1 Out-of-band physical-state audit

The post-seal audit has three deliberately separate readers. The independent
Python oracle and scalable base-edge SQL oracle each emit an expected physical
projection derived only from frozen lifecycle, normalized text, observation,
currency, policy, ownership, and semantic source relations. Their query/code
definitions MUST contain no D25 relation name. A third read-only actual-image
adapter reads only the sealed D25 current-image/current relations. The audit
comparator, outside all three readers and outside measured event latency,
compares their returned values. Giving either semantic oracle a D25 row, hash,
count, patch, or accumulator as an input is forbidden.

All three database reads MUST share one exported PostgreSQL `REPEATABLE READ`
snapshot pinned after the target sealed heads are locked/read. The Python
oracle receives only source rows materialized from that snapshot; the SQL
expected reader and actual-image reader import the same snapshot in read-only
transactions. The coordinator rechecks the target head after collection. A
snapshot-import failure, head mismatch, or overlapping later-seal ambiguity is
an invalid audit that must restart, never a mismatch or PASS.

The expected and actual semantic projections use these exact row preimages:

```text
AUDIT_OBSERVATION =
  SEQ((TEXT(observation_id), TEXT(requirement_version_id),
       TEXT(group_version_id), INT(requirement_ordinal), HASH(text_hash)))

AUDIT_EDGE =
  SEQ((TEXT(requirement_version_id), HASH(text_hash),
       TEXT(group_version_id), INT(requirement_ordinal), INT(refcount)))

AUDIT_MASK =
  SEQ((TEXT(group_version_id), HASH(text_hash), INT(mask)))

AUDIT_HALL =
  SEQ((TEXT(group_version_id), INT(requirement_count),
       SEQ(INT(value) for value in mask_histogram),
       SEQ(INT(value) for value in neighbor_counts),
       SEQ(INT(value) for value in deficiencies),
       INT(maximum_deficiency), INT(matching_size),
       INT(distinct_hash_count)))
```

The family rank is exactly `observation=0`, `edge=1`, `mask=2`, `hall=3`;
lexical enum-value order is forbidden. Audit projection rows and mismatches
sort and pair one-to-one only by the canonical outer physical key:
observation ID; edge `(requirement_version_id,text_hash)`; mask
`(group_version_id,text_hash)`; and Hall group ID. Duplicate outer keys are an
invalid audit. Edge group ID and requirement ordinal remain validated
`AUDIT_EDGE` payload, never sort/pair coordinates. A decodable key with a
fully decodable but different payload produces one ordinary keyed mismatch. A
row whose unique outer key decodes but whose complete `AUDIT_ROW` payload
cannot decode and canonically re-encode produces the exact keyed
`malformed_payload` mismatch defined below and makes the whole actual-
projection digest absent. An outer key that cannot decode as the typed
`AUDIT_KEY`, or a duplicate outer key, is a typed invalid-audit failure for
which no audit artifact or digest may be reported; it is never serialized
through an invented key. This audit order is separate from and does not change
the Section-4.7 patch-change order. Hall arrays retain numeric subset order.

The exact typed outer audit keys are:

```text
AUDIT_KEY(observation) = SEQ((TEXT(observation_id)))
AUDIT_KEY(edge) =
  SEQ((TEXT(requirement_version_id), HASH(text_hash)))
AUDIT_KEY(mask) =
  SEQ((TEXT(group_version_id), HASH(text_hash)))
AUDIT_KEY(hall) = SEQ((TEXT(group_version_id)))
```

For each family `f`, with literal enum value
`observation | edge | mask | hall`:

```text
family_digest[f] = stable_m5_digest(
  "m5-persisted-matching-physical-audit-family-v1",
  *ENUM(f), *INT(row_count), *SEQ(each complete row preimage))

semantic_projection_digest = stable_m5_digest(
  "m5-persisted-matching-physical-audit-projection-v1",
  *INT(head_epoch_id), *INT(head_revision),
  *TEXT(decision_policy_version),
  *HASH(family_digest[observation]), *HASH(family_digest[edge]),
  *HASH(family_digest[mask]), *HASH(family_digest[hall]))
```

`family_digest` and `semantic_projection_digest` exist only when every row on
that side has one complete canonical `AUDIT_ROW` encoding. The Python and SQL
expected readers MUST always satisfy that condition or the audit is invalid
with no artifact. A keyed malformed actual payload follows the optional failed-
artifact branch below instead of inventing a family-row encoding.

The Python and SQL expected projections MUST be byte-identical before either
is compared with the actual projection. The actual adapter MUST first require
one current-image header whose epoch equals both locked publication-head epoch
IDs, whose revision equals the M5 head's `sealed_revision` and sealed epoch
revision, and whose policy equals the strict current policy. It projects every
current row, not a sample. A missing, extra, duplicated, reordered, malformed,
or different validly encoded row changes a family digest and fails the audit.
A keyed malformed row follows the exact absent-actual-projection branch below;
an invalid/duplicate outer key is the no-artifact invalid audit above.

The keyed-malformed-current branch has precedence over provenance replay. In
that exact branch the comparator completes all canonically keyed physical
mismatches but MUST NOT pass the malformed current row to `CURRENT_PROVENANCE`
or run the provenance pass. The retained failed artifact has exactly
`provenance_ok=false`, `provenance_replay_digest=None`, all four working-image/
accumulator expected/actual provenance digests `None`, and
`provenance_mismatches=()`. This is not a provenance PASS or an omitted success;
the typed `actual_error=malformed_payload` is the controlling failure.

Physical-only installed coordinates, retained working history, working-image
headers, and matching-work accumulators are checked by a separate provenance
pass because they are not semantic-oracle outputs. This pass may read D25
relations because it is an integrity audit, not an independent expected-state
oracle or a measured transition.

The pass decodes and rehashes every applicable Section-4.7 child/outer
preimage, then replays patches per epoch in
`(epoch_id,resulting_revision,source_kind,source_id)` order from the accepted
bootstrap. It first reconstructs and compares every retained working row for
sealed, failed, and nonterminal epochs. For a sealed epoch it then applies the
one deterministic seal promotion: a final present/positive working value
becomes a current-layer value with `installed_epoch_id=epoch_id` and
`installed_revision=terminal_seal_revision`; a tombstone deletes the current
value. Failed and nonterminal epochs never promote. Replay across sealed epochs
must produce the exact current rows and installed coordinates at the audited
head.

For every D25 runtime epoch the pass independently derives the expected
working-image header: its base is the current image at structural open, its
policy is the immutable update policy, its status and optional terminal
revision come from the checked runtime/epoch rows, and its `updated_revision`
is the greatest committed D25 contribution revision. It compares every header
field and rejects a missing or extra row. Separately, it component-wise sums
all 37 decoded, validated contribution vectors for the epoch in contribution-
revision order, recomputes the Section-4.6 `m5-matching-work-v1` digest, and
requires the accumulator's complete vector and digest to equal that sum and
its `updated_revision` to equal the greatest contribution revision. A
consistently re-digested but altered accumulator still fails this independent
sum.

A malformed digest, missing or extra patch/contribution/header/accumulator,
contribution mismatch, terminal-state mismatch, retained-working mismatch, or
current installed-coordinate mismatch is an audit failure. No value from an
accumulator is permitted to seed its expected sum.

After validating every child and contribution, the provenance pass encodes:

```text
PATCH_PROVENANCE =
  SEQ((INT(epoch_id), INT(resulting_revision), ENUM(source_kind),
       TEXT(source_id), ENUM(nonterminal|failed|sealed),
       HASH(patch_digest), HASH(contribution_digest)))

WORKING_IMAGE_PROVENANCE =
  SEQ((INT(epoch_id), ENUM(nonterminal|failed|sealed),
       OPTION(INT(terminal_revision)), INT(base_epoch_id),
       INT(base_revision), TEXT(decision_policy_version),
       INT(updated_revision), HASH(last_patch_digest)))

MATCHING_WORK_VECTOR =
  the exact 37 INT fields from contribution_additions through
  public_status_deltas in the displayed Section-4.6 order

ACCUMULATOR_PROVENANCE =
  SEQ((INT(epoch_id), ENUM(nonterminal|failed|sealed),
       MATCHING_WORK_VECTOR, HASH(matching_work_digest),
       INT(updated_revision)))

CURRENT_PROVENANCE =
  SEQ((ENUM(observation|edge|mask|hall), KEY(family),
       CURRENT_POINT(family), OPTION(HASH(last_touch_patch_digest))))

WORKING_PROVENANCE =
  SEQ((ENUM(observation|edge|mask|hall), KEY(family),
       WORKING_POINT(family), HASH(last_touch_patch_digest)))

provenance_replay_digest = stable_m5_digest(
  "m5-persisted-matching-provenance-replay-v1",
  *INT(head_epoch_id), *INT(head_revision),
  *SEQ(PATCH_PROVENANCE in the replay order above),
  *SEQ(WORKING_IMAGE_PROVENANCE in epoch order),
  *SEQ(ACCUMULATOR_PROVENANCE in epoch order),
  *SEQ(CURRENT_PROVENANCE in family-rank/key order),
  *SEQ(WORKING_PROVENANCE in epoch/family-rank/key order))
```

`MATCHING_WORK_VECTOR` is a literal macro splice of those 37 `INT(...)`
arguments, not one text, sequence, JSON, or composite field. The expected and
actual encoders expand the same 37 arguments before hashing.

`KEY` and `*_POINT` are exactly the outer-key and point encodings in Section
4.7. Family rank is the exact rank frozen above. `last_touch_patch_digest` is
`None` only for a current row created by the accepted activation/bootstrap,
whose installed coordinates MUST equal that bootstrap point. For every later
current row it names the patch responsible for the effective working value
that the deterministic seal transformation promoted; the patch did not itself
produce the different current-layer encoding. Every retained working row names
the patch that produced its exact working bytes. The same contribution row
reached through its source key and resulting-revision key is one
`PATCH_PROVENANCE` record, not two. A failed decode returns a typed audit
failure rather than hashing a partially trusted row set. This sentence applies
to the ordinary fully encoded semantic-projection path; the earlier keyed-
malformed-current branch takes precedence and skips provenance exactly as
specified there.

The four pairwise digests use these exact calls:

```text
working_image_expected_provenance_digest = stable_m5_digest(
  "m5-persisted-matching-working-image-provenance-v1",
  *SEQ(WORKING_IMAGE_PROVENANCE(row)
       for row in expected working-image epoch order))

working_image_actual_provenance_digest = stable_m5_digest(
  "m5-persisted-matching-working-image-provenance-v1",
  *SEQ(WORKING_IMAGE_PROVENANCE(row)
       for row in actual working-image epoch order))

accumulator_expected_provenance_digest = stable_m5_digest(
  "m5-persisted-matching-accumulator-provenance-v1",
  *SEQ(ACCUMULATOR_PROVENANCE(row)
       for row in expected accumulator epoch order))

accumulator_actual_provenance_digest = stable_m5_digest(
  "m5-persisted-matching-accumulator-provenance-v1",
  *SEQ(ACCUMULATOR_PROVENANCE(row)
       for row in actual accumulator epoch order))
```

The outer `SEQ` framing, including its zero-row count, is mandatory; directly
flattening rows into the domain call is forbidden. Equality of each expected/
actual pair is a compact check only; the audit must also compare every row and
field.

For a per-epoch mismatch, the working-image row digest uses domain
`m5-persisted-matching-working-image-provenance-row-v1` followed by the exact
fields inside one `WORKING_IMAGE_PROVENANCE` row. The accumulator row digest
uses domain `m5-persisted-matching-accumulator-provenance-row-v1` followed by
the exact fields inside one `ACCUMULATOR_PROVENANCE` row. No sequence wrapper,
field, or status value may be inferred or omitted.

The evaluation harness returns and retains:

```text
M5PersistedMatchingPhysicalAudit(
  head_epoch_id,
  head_revision,
  decision_policy_version,
  python_expected_projection_digest,
  sql_expected_projection_digest,
  actual_projection_digest?,
  actual_error?,
  provenance_ok,
  provenance_replay_digest?,
  working_image_expected_provenance_digest?,
  working_image_actual_provenance_digest?,
  accumulator_expected_provenance_digest?,
  accumulator_actual_provenance_digest?,
  mismatches,
  provenance_mismatches,
  audit_digest
)
```

Each mismatch is:

```text
M5PersistedMatchingPhysicalMismatch(
  family, key, expected_row_digest?, actual_row_digest?, actual_error?
)

row_digest = stable_m5_digest(
  "m5-persisted-matching-physical-audit-row-v1",
  *ENUM(family), *AUDIT_ROW(family))

physical_mismatch_preimage =
  SEQ((ENUM(family), AUDIT_KEY(family),
       OPTION(HASH(expected_row_digest)),
       OPTION(HASH(actual_row_digest)),
       OPTION(ENUM(actual_error))))

M5PersistedMatchingProvenanceMismatch(
  kind, epoch_id, expected_row_digest?, actual_row_digest?
)

provenance_mismatch_preimage =
  SEQ((ENUM(kind), INT(epoch_id),
       OPTION(HASH(expected_row_digest)),
       OPTION(HASH(actual_row_digest))))
```

`mismatches` is sorted by the exact family rank above and then the exact within-
family order. Its `key` is the typed `AUDIT_KEY(family)` value, never an
opaque byte string. `provenance_mismatches` admits exact kind values
`working_image | accumulator`, with rank `working_image=0` and
`accumulator=1`, then sorts by epoch ID. A genuinely absent physical row uses
`OPTION(None)` for that side's row digest. The only admitted physical-mismatch
`actual_error` enum value is literal `malformed_payload`. A genuinely absent
actual row has
`actual_row_digest=None, actual_error=None`; a keyed malformed actual row has
`actual_row_digest=None, actual_error=malformed_payload`; and a canonically
encoded actual row has a present digest and no error. Every other combination
is invalid. A malformed expected row, or a present provenance row that cannot
decode completely, makes the audit a typed no-artifact invalid audit; neither
is represented as an absent row. Raw `bytea`, JSON, `repr`, or an untyped
preimage is forbidden in either mismatch digest.

At artifact level exactly one actual-projection shape is valid:

1. every actual row is canonically encoded, so
   `actual_projection_digest=Some(...)` and `actual_error=None`; or
2. at least one uniquely keyed actual row has an undecodable/noncanonical
   payload, so `actual_projection_digest=None` and
   `actual_error=Some(malformed_payload)`, with one keyed malformed mismatch
   for every such row, plus the exact skipped-provenance values frozen above.

Missing/extra but otherwise canonical rows use shape 1. Invalid/duplicate
outer keys and current-image-header precondition failures remain typed invalid
audits with no artifact. The audit digest is:

```text
audit_digest = stable_m5_digest(
  "m5-persisted-matching-physical-audit-v1",
  *INT(head_epoch_id), *INT(head_revision),
  *TEXT(decision_policy_version),
  *HASH(python_expected_projection_digest),
  *HASH(sql_expected_projection_digest),
  *OPTION(HASH(actual_projection_digest)),
  *OPTION(ENUM(actual_error)), *BOOL(provenance_ok),
  *OPTION(HASH(provenance_replay_digest)),
  *OPTION(HASH(working_image_expected_provenance_digest)),
  *OPTION(HASH(working_image_actual_provenance_digest)),
  *OPTION(HASH(accumulator_expected_provenance_digest)),
  *OPTION(HASH(accumulator_actual_provenance_digest)),
  *SEQ(physical_mismatch_preimage in the frozen order),
  *SEQ(provenance_mismatch_preimage in the frozen order))
```

`PASS` requires a present actual projection digest, no actual error, all three
projection digests equal, an empty mismatch tuple,
`provenance_ok=true`, a present provenance digest, both present expected/actual
working-image digests equal, both present expected/actual accumulator digests
equal, and an empty provenance-mismatch tuple. The artifact is evaluation
evidence only; it is not written into a runtime, semantic, patch, contribution,
result, or publication identity and its work/time is reported separately.

## 7. Deterministic certificate representatives

The store MUST expose two indexed effective queries:

```text
least_effective_observation(
  epoch_id, group_version_id, requirement_ordinal, text_hash
) -> observation_id | NULL

representative_effective_hashes(
  epoch_id, group_version_id, mask, limit
) -> tuple[text_hash, ...]
```

`least_effective_observation` returns the least active observation ID under
`COLLATE "C"` after any working row with the same observation key shadows the
current row. When the caller has already proved a selected edge has positive
effective refcount, NULL is an invariant failure and the entire transition
MUST roll back.

`representative_effective_hashes` requires
`limit=requirement_count`, excludes tombstones and shadowed current rows, and
returns **exactly** the least `min(C[mask], requirement_count)` hashes under
`COLLATE "C"`. Fewer or more rows than the effective Hall histogram requires
is an invariant failure, not a short result. A current hash is excluded if any
working row exists for that `(epoch,group,hash)`, including a zero tombstone or
a row whose new positive mask differs from the queried current mask.
Certificate construction iterates masks in numeric ascending order,
requirements in ordinal order, and candidate hashes in byte order.

Retaining a certificate validates its at most eight selected rows by point
membership. Provenance repair reads one least observation per selected edge.
Reconstruction reads at most `r_g` hashes from each of at most
`2^r_g-1` nonzero masks. No certificate path may scan total witness degree.
Both representative queries require the already-held runtime-epoch and image
locks; they are not standalone eventually-consistent reads.

## 8. Cursor-local persistence API

The public `M5TypedApplication.run_event` API remains unchanged. The existing
runtime persistence methods remain transaction owners. Migration 017 adds
these persistence-internal cursor-local operations only:

```text
effective_matching_image(
  cursor, epoch_id
) -> M5MatchingImagePoint

resolved_matching_observation_point(
  cursor, epoch_id, observation_id
) -> M5MatchingObservationPoint | None

resolved_matching_edge_point(
  cursor, epoch_id, requirement_version_id, text_hash
) -> M5MatchingEdgePoint | None

resolved_matching_mask_point(
  cursor, epoch_id, group_version_id, text_hash
) -> M5MatchingMaskPoint | None

resolved_matching_hall_point(
  cursor, epoch_id, group_version_id
) -> M5MatchingHallPoint | None

effective_matching_observation(
  cursor, epoch_id, observation_id
) -> M5MatchingObservationPoint | None

effective_matching_edge(
  cursor, epoch_id, requirement_version_id, text_hash
) -> M5MatchingEdgePoint | None

effective_matching_mask(
  cursor, epoch_id, group_version_id, text_hash
) -> int

effective_matching_hall(
  cursor, epoch_id, group_version_id
) -> HallMaskState | None

least_effective_observation(
  cursor, epoch_id, group_version_id, requirement_ordinal, text_hash
) -> str | None

representative_effective_hashes(
  cursor, epoch_id, group_version_id, mask, limit
) -> tuple[str, ...]

derive_matching_transition_intent(
  cursor, epoch_id, expected_runtime_revision, resulting_revision,
  source_kind, source_id,
  expected_source_identity_hash: SHA256 | None = None
) -> M5PersistedMatchingTransitionIntent

apply_matching_transition(
  cursor, intent: M5PersistedMatchingTransitionIntent,
  expected_patch_digest: SHA256 | None = None,
  expected_work: M5OverlayWork | None = None
) -> M5PersistedMatchingPatchReceipt

promote_matching_overlay(
  cursor, epoch_id, expected_revision, sealed_revision
) -> None

current_matching_work(cursor, epoch_id) -> M5OverlayWork
```

`M5MatchingImagePoint` is the exact lossless composite:

```text
M5MatchingImagePoint(
  current_decision_policy_version,
  current_installed_epoch_id,
  current_installed_revision,
  working_epoch_id,
  working_base_epoch_id,
  working_base_revision,
  working_decision_policy_version,
  working_updated_revision
)
```

Its fields appear in that order and equal the already-locked Section-4.1
current and requested-epoch working headers. It is not an ambient mapping or
serialization and it cannot omit either header.

Every `M5MatchingObservationPoint`, `M5MatchingEdgePoint`,
`M5MatchingMaskPoint`, and `M5MatchingHallPoint` is a lossless tagged union of
the corresponding `*_CURRENT` and `*_WORKING` Section-4.7 recipes, in exactly
that field order. It retains the layer tag and every epoch/install coordinate,
payload, `present` flag or zero value, and revision; no ambient dataclass
serialization defines it.

A `resolved_*_point` result of `None` means neither a working row nor an
unshadowed current row exists. A persisted working observation/Hall
`present=false`, edge `refcount=0`, or mask `mask=0` tombstone remains a
present tagged working point. The `effective_*` value helpers apply the
Section-5 eligibility filter after resolution: they return `None` for a
filtered observation, edge, or Hall value and integer zero for a filtered mask
value. Transition derivation, before-image validation, replay, and physical
audit MUST use the resolved point operations and MUST NOT infer physical
absence from a filtered effective value. A synthetic zero point is forbidden.

The transition intent is a byte-total, persistence-internal lock plan, not a
patch or result assertion:

```text
M5PersistedMatchingTransitionIntent(
  source_kind,
  source_id,
  source_identity_hash,
  before_epoch_id,
  before_revision,
  resulting_epoch_id,
  resulting_revision,
  decision_policy_version,
  group_shapes,
  observation_ids,
  edge_keys,
  mask_keys,
  hall_group_ids,
  requirement_state_ids,
  group_state_ids,
  claim_state_ids,
  answer_state_ids,
  group_certificate_ids,
  claim_certificate_ids,
  intent_digest
)
```

Every tuple is sorted and unique under the Section-4.7 order. `group_shapes`
uses the exact group-shape entry encoding from Section 4.7. Each edge key is
`(group_version_id, requirement_ordinal, text_hash,
requirement_version_id)`; each mask key is `(group_version_id, text_hash)`;
every other tuple contains the named text ID. The digest is exactly:

```text
intent_digest = stable_m5_digest(
  "m5-persisted-matching-transition-intent-v1",
  *ENUM(source_kind), *TEXT(source_id), *HASH(source_identity_hash),
  *INT(before_epoch_id), *INT(before_revision),
  *INT(resulting_epoch_id), *INT(resulting_revision),
  *TEXT(decision_policy_version),
  *SEQ(each group_shapes entry in its exact Section-4.7 typed encoding),
  *SEQ(TEXT(value) for value in observation_ids),
  *SEQ(each edge key encoded as
       SEQ((TEXT(group_version_id), INT(requirement_ordinal),
            HASH(text_hash), TEXT(requirement_version_id)))),
  *SEQ(each mask key encoded as
       SEQ((TEXT(group_version_id), HASH(text_hash)))),
  *SEQ(TEXT(value) for value in hall_group_ids),
  *SEQ(TEXT(value) for value in requirement_state_ids),
  *SEQ(TEXT(value) for value in group_state_ids),
  *SEQ(TEXT(value) for value in claim_state_ids),
  *SEQ(TEXT(value) for value in answer_state_ids),
  *SEQ(TEXT(value) for value in group_certificate_ids),
  *SEQ(TEXT(value) for value in claim_certificate_ids))
```

`derive_matching_transition_intent` MUST load the immutable persisted source
named by `source_kind` and `source_id`, derive `source_identity_hash` itself,
and derive every affected key from that source, locked lifecycle/currency/
observation rows, and bounded indexed discovery. An optional expected source
hash is compare-only and is discarded. The method MUST NOT accept any caller-
supplied affected tuple, physical before/after value, logical after value,
certificate choice, patch child, output digest, or work counter.
`apply_matching_transition` MUST recompute the complete intent from those same
locked rows, compare every field and `intent_digest`, and only then derive all
physical and logical before/after values, representative choices, exact
indexed-operation counters, the Section-4.7 patch, and the contribution. A
stale or forged intent conflicts before the first write.

The receipt contains the complete store-computed
`M5PersistedMatchingPatch`, its contribution digest, exact accumulated work,
resulting revision, and an `exact_replay` flag. The patch and receipt use the
exact Section-4.7 source, point, preimage, logical-output, work, and digest
contract. `expected_patch_digest`, when present, is compared with the computed
patch only after full derivation and before the first write; it is discarded
and never becomes persisted authority. An optional expected work value has the
same compare-only status. The store, not an external worker or application
caller, derives every persisted `M5OverlayWork` field.

For a later transition, `resulting_revision` MUST equal
`expected_runtime_revision+1`, and the patch before point is
`(epoch_id, expected_runtime_revision)`. For structural-open, both runtime
arguments are 1 under the exact authorization sequence above, while the patch
before point is the sealed predecessor; no runtime revision-zero row or lock
is invented. `current_matching_work` is cursor-local here; the public read
store may wrap it in a separate read-only transaction.

None of these cursor-local methods may commit, roll back, increment the epoch
independently, open a nested transaction, perform an external/model call, or
read a full oracle. They MUST assert that the runtime-epoch/image locks are
already held. `current_matching_work` is read-only and returns the retained
accumulator for both nonterminal and terminal epochs.

Two affected-set range adapters are permitted:

- structural group replacement/retirement may enumerate the exact effective
  physical rows for only the named affected group; and
- seal/reconnect may enumerate working rows for only one epoch through an
  `epoch_id`-prefix index.

Neither is a global repository, oracle, or job scan. Structural enumeration is
charged to the frozen `W_g + E_g + H_g` term; seal enumeration is charged to
touched rows and PostgreSQL physical work. No adapter may construct or retain
an `M5IncrementalOverlay` or another process-local index. Reconnect's epoch
range is only for retained-history/touched-key scheduling; every subsequent
transition still reloads its exact points under its own transaction locks.

## 9. Transaction behavior

### 9.1 Structural open

The revision-1 typed-open transaction MUST install all matching effects that
are already determined before external work:

- every structural form creates the working-image header and one immutable
  `structural_open` patch/contribution. A no-change form uses empty change and
  binding sequences plus zero applicable operation counters, but still
  records the mandatory 71-byte empty logical-output image from Section 4.6;
- document insert normally has no requirement-membership change but still
  records that exact byte-total joint structural patch;
- document delete/replace removes exactly the withdrawn current requirement
  observations named by the frozen withdrawal plan;
- standalone claim `ObserveEvent` atomically coalesces its accepted direct-M4
  observation/currency/state/certificate transition with the derived
  combined-v2 claim/answer/certificate transition in the same revision-1
  `structural_open` patch/contribution. It creates no separate
  `direct_transition`;
- rootless `ObserveRequirementEvent` may reference an existing inactive
  requirement or chunk. Eligibility is evaluated at the resulting point. An
  inactive observation is archived with `eligible_for_currency=false`, does
  not supersede currency, and changes no matching, requirement, group, claim,
  answer, or certificate state. It still receives the one mandatory byte-total
  structural patch/contribution. An active noncanonical-task observation may
  hold its own typed currency key but remains matching-inert. Only an active
  canonical `verify_requirement_v1` observation applies the coalesced old-
  holder removal/new-holder policy-relative addition: SUPPORT may add the new
  membership, while REFUTE or NEUTRAL adds none; every active canonical label
  removes a superseded SUPPORT holder when applicable. The rootless
  transaction invokes no model;
- group registration creates one present empty Hall row and empty working
  requirement/group semantic state for the successor;
- group replacement/retirement applies one coalesced before/after structural
  batch over only the named affected group, tombstones every predecessor
  membership/edge/mask/Hall row, installs the empty successor Hall row for
  replacement, and never exposes a transient partially retired group;
- policy change performs the exact ordered score-range probes, applies every
  decision flip, records even an empty probe, and rebinds every required
  complete-group and typed-claim certificate under the frozen policy rules;
  the working-image policy changes even when all physical rows are numerically
  unchanged, while the existing frozen semantic patch remains responsible for
  every migration-014 state row whose policy identity changes; and
- the exact byte-total patch and structural matching work initialize the D25
  artifact/contribution ledger and accumulator atomically.

No retrieval, embedding, or verifier call occurs in this transaction.

### 9.2 Active verifier completion

Inside the existing one verifier-completion transaction, the store MUST:

1. reserve and validate the attempt output and D24 work/timing;
2. lock the epoch, runtime header, job, attempt, input, activity, and exact
   currency keys;
3. classify activity at one snapshot;
4. archive the immutable verifier artifact, observation, execution, and
   attempt-result rows;
5. for an active completion, coalesce old-holder removal and new-holder
   addition before changing physical state;
6. load only the affected observation, edge, mask, Hall, semantic-state, and
   certificate points;
7. compute and validate the joint byte-total physical/logical D25 patch and
   store-derived work;
8. apply the physical matching patch and existing semantic/certificate patch;
9. insert the immutable patch artifact/contribution, update affected combined
   claim/answer state and PENDING, and advance the D25 image/accumulator;
10. add the separate D24 runtime work/timing without using the D25 contribution
    as another timing anchor; and
11. terminalize the job and advance the epoch revision exactly once.

A positive-to-positive multiplicity change MUST skip mask/Hall mutation while
still repairing selected observation provenance when needed. Requirement
REFUTE and NEUTRAL remain archived but create no witness edge and no parent
refutation.

Inactive completion and terminal audit-only completion MUST perform no
currency, physical matching, semantic state, certificate, D25 image, patch,
contribution, or counter mutation.

### 9.3 Structural withdrawal and policy batches

Document withdrawal MUST consume exact stored observation IDs and perform
point lookups; it MUST NOT rediscover withdrawals from a model or hidden
reserve. Retiring one group may use only its affected-group range adapter.
Register, replace, retire, document delete, and document replace MUST each bind
their exact group shape/withdrawal before images in the structural patch, so
replay under a changed withdrawal or predecessor image conflicts.

A policy event begins from one sealed predecessor with no other live epoch.
Candidate enumeration MUST use the existing ordered observation score indexes
and current typed currency. It may be dense and MUST report `P`, `U`, database
I/O, and output honestly. A zero-decision-flip policy change still records its
ordered probes and performs required certificate policy rebindings, while
`hash_mask_transitions` and Hall-change counters remain zero.
The structural patch binds both policy identities through its predecessor
image and new working-image policy; a changed policy with the same numeric
threshold result cannot alias the prior image.

### 9.4 Typed seal

Measured seal MUST use only persisted point/CAS invariants and the one-epoch
working-row ranges. It MUST validate:

- expected revision and semantic completeness;
- D24 coordination/work readiness;
- nonnegative physical refcounts;
- every touched Hall row's bounded local consistency;
- exact Hall scalar agreement with working group state;
- valid open group/claim certificate bindings and selected provenance;
- zero PENDING;
- complete D24 revision coverage; and
- one valid structural D25 contribution plus exact checked-procedure coverage
  for every active matching/combined-state semantic transition.

D25 accumulator/image `updated_revision` is the latest D25 semantic revision
and MUST NOT be required to equal the runtime revision: acquisitions, root
staging/barriers, cancellation, inactive/audit-only completion, failure, and
seal may advance or terminate the runtime without a D25 transition. Coverage
is enforced when each semantic mutator commits its unique Section-4.7
contribution, not by scanning jobs or contributions at seal.

In the same transaction, seal MUST promote the physical image header/current
state, migration-014 semantic materialized/published state, certificate
bindings, currency, direct M4 state, public deltas, changed-state references,
both publication heads, event result, and epoch terminal state. Physical rows
MUST NOT create a seventh changed-state-reference kind. Patch artifacts and
contributions remain immutable epoch history and are never promoted or
deleted.

Seal MUST NOT instantiate `M5IncrementalOverlay`, invoke Python full
recomputation, either SQL oracle, runtime pure-transition reconstruction, full
repository hydration, a process cache, or aggregate job/contribution scans.
The independent three-oracle audit runs only after commit and its time is
excluded from measured seal latency.

### 9.5 Terminal failure

Failure serializes on the epoch/runtime row, terminalizes remaining work under
the frozen D23/D24 rules, and stores the durable failed result. It MUST retain
all D25 working-image/working/tombstone rows, patch artifacts, contributions,
and the matching-work accumulator. It MUST NOT promote or delete them, update
the current image/physical rows, update materialized or published semantic
state, change a certificate pointer, or advance either head. Once the epoch is
terminal, its D25 rows are immutable.

A later epoch reads the unchanged current baseline plus only its own working
rows. It MUST never overlay rows belonging to a failed or sealed predecessor.

### 9.6 Reconnect and replay

Reconnect to a nonterminal epoch MUST use a fresh-store-compatible path,
hydrate jobs from the runtime tables, and derive matching state only from the
image and effective point queries. It MUST reuse committed staged root results
and committed verifier completions and execute only genuinely missing external
work under D24's recoverable lease rules. No retained object from the prior
process is an input.

Exact replay of a committed semantic transition, seal, or failure MUST point-
read and validate its stored source, patch preimage/digest, contribution, and
terminal identities and return without changing physical rows, semantic rows,
revision, D24 work/timing, D25 image, or D25 matching work. A differing output,
logical patch, work image, policy, group shape, or expected physical before-
state MUST conflict and roll back. Replay never reconstructs history from the
current final working rows.

## 10. Total lock order amendment

The runtime-addendum lock order MUST be refined, not replaced:

1. tiers 1 through 10 remain unchanged;
2. tier 11a: semantic observations, then working/current currency keys in the
   frozen typed-key order;
3. tier 11b: current then working matching-image rows;
4. tier 11c: matching observation rows by observation ID, current before
   working for one key;
5. tier 11d: matching edge rows by group ID, requirement ordinal, then hash,
   current before working for one key;
6. tier 11e: matching mask rows by group ID, then hash, current before working
   for one key;
7. tier 12a: matching Hall rows by group ID, current before working for one
   key;
8. tier 12b: existing requirement state, group state, and group-certificate
   bindings;
9. tiers 13 through 14 remain unchanged; and
10. tier 15 is refined into this complete suborder, with no unnamed row class
    between entries:
    1. `15a` existing `groundloop_m5_owner_pending_counter` rows by owner
       claim ID;
    2. `15b` existing `groundloop_m5_answer_pending_counter` rows by answer
       version ID;
    3. `15c` the unchanged compact-evaluation rows in this order:
       `groundloop_m4_evaluation_epoch_counter` by epoch,
       `groundloop_m4_evaluation_override_counter` by
       `(epoch_id, object_type, object_id)`, then
       `groundloop_m4_evaluation_counter_transition` by
       `(epoch_id, transition_id)` and its unique `(epoch_id, to_revision)`
       conflict key;
    4. `15d` `groundloop_m5_runtime_work_contribution` rows by their complete
       immutable contribution key;
    5. `15e` the `groundloop_m5_runtime_work_accumulator` epoch row;
    6. `15f` `groundloop_m5_runtime_timing_contribution` rows by their complete
       point key;
    7. `15g` `groundloop_m5_transition_call_timing` rows by their complete
       anchor key;
    8. `15h` the `groundloop_m5_runtime_timing_accumulator` epoch row;
    9. `15i` `groundloop_m5_matching_patch_artifact` rows by patch digest;
    10. `15j` `groundloop_m5_matching_work_contribution` rows by
        `(epoch_id, source_kind, source_id)`, then the same table's unique
        `(epoch_id, resulting_revision)` conflict key; and
    11. `15k` the `groundloop_m5_matching_work_accumulator` epoch row.

`groundloop_m5_event_timing_coverage` remains with its one-to-one event result
at unchanged tier 16, followed by the other unchanged public delta, changed-
state-reference, event-result, and publication rows.

The labels `15a` through `15k` refine the accepted tier; they do not create
new outer tiers. A transaction that needs more than one row in any sub-tier
locks all of them in ascending numeric/key-component order and then ascending
UTF-8 byte order under `COLLATE "C"`. Insert-on-absence paths first lock the
canonical advisory or unique-index conflict key at that same position; they
MUST NOT rely on whichever unique constraint PostgreSQL happens to probe
first. D25 never reacquires a `15a` through `15h` row after reaching `15i`.
The one contribution row found through both its source primary key and
resulting-revision unique key is locked and validated once at `15j`. Absence of
either contribution key is safe for insertion only while the tier-6 epoch row
lock is held, because that lock serializes every semantic transition and its
revision in one epoch; a missing-row probe without that lock is forbidden.
Cross-epoch patch-artifact reuse serializes on the digest advisory/conflict key
at `15i` before insert-or-validate.

All keys knowable before representative discovery MUST be gathered and sorted
before the first tier-11 lock. The only dynamic-key exception is certificate
representative discovery: after locking current then working image rows at tier
11b, the transaction may perform the Section-7 indexed point/range reads under
the already-held tier-6 epoch lock, compute the bounded candidate image, and
gather and sort its exact observation keys before acquiring tier 11c. It then
locks those rows in the displayed tier order, revalidates every before image,
and performs no D25 write until all required tier-11c-through-12b locks are
held. It may not discover another earlier-tier key after that point. A
transaction MUST NOT otherwise acquire an earlier tier after a later tier.
Completion still begins at tier 5. Seal still begins at tier 1. Every D25
effective or representative query MUST assert the already-held epoch and image
rows; the epoch row serializes semantic microtransactions and prevents
representative-query phantoms from another completion.

The migration-017 installer MUST own one top-level, read-write PostgreSQL
`READ COMMITTED` transaction from its initial ledger read through commit. It
MUST reject invocation inside an ambient transaction, nested transaction or
savepoint, and MUST reject any other isolation level. Consequently the
mandatory post-lock ledger query below receives a fresh statement snapshot and
can observe an installer that committed after the initial read. Lock-not-
available, connection, or transaction errors abort the whole transaction;
there is no in-transaction retry or serialization-error reinterpretation.

Migration 017 applies its ledger-first exact-rerun/conflict decision and exact
accepted-016 validation before the installation lock phase. For a first
installation it attempts `ACCESS EXCLUSIVE MODE NOWAIT` table locks in exactly
this order before any singleton row read, history check, DDL, or backfill:

```text
groundloop_runtime_mode
groundloop_m4_publication_head
groundloop_m5_publication_head
groundloop_m5_activation
groundloop_epoch
groundloop_m5_runtime_epoch
groundloop_m5_update
groundloop_decision_policy
groundloop_document
groundloop_document_version
groundloop_chunk_version
groundloop_answer_version
groundloop_claim
groundloop_m5_group_family
groundloop_m5_group_version
groundloop_m5_requirement_version
groundloop_m5_group_validity
groundloop_m5_group_deactivation
groundloop_m5_group_family_retirement
groundloop_semantic_subject
groundloop_semantic_observation
groundloop_observation_currency
groundloop_published_observation_currency
groundloop_claim_state_materialized
groundloop_answer_state_materialized
groundloop_claim_certificate
groundloop_published_claim_state
groundloop_published_answer_state
groundloop_m5_requirement_state_materialized
groundloop_m5_group_state_materialized
groundloop_m5_claim_state_materialized
groundloop_m5_answer_state_materialized
groundloop_m5_published_requirement_state
groundloop_m5_published_group_state
groundloop_m5_published_claim_state
groundloop_m5_published_answer_state
groundloop_m5_group_certificate_artifact
groundloop_m5_group_certificate_artifact_row
groundloop_m5_claim_certificate_artifact
groundloop_m5_published_group_certificate_binding
groundloop_m5_published_claim_certificate_binding
```

This deliberately strengthens the migration-015/016 installation mode. It is
a fail-fast maintenance barrier, not an availability claim. `ACCESS EXCLUSIVE
... NOWAIT` conflicts before waiting with every pre-existing reader/mutator
lock on a tuple relation. If any lock in the ordered prefix is unavailable,
the whole transaction MUST abort and release the prefix; it MUST NOT wait,
savepoint-retry, or continue. A caller may retry only in a new transaction
starting again from the ledger-first decision. No installation query may lock
or read a later-listed relation and then acquire an earlier-listed one.

After every table lock is held, the installer MUST re-read the migration-017
ledger row before any singleton or history read. An exact row returns the
ordinary no-op result, a same-ID different row conflicts, and only continued
absence may proceed. This closes the race in which another installer commits
between the initial ledger read and this installer's first lock. The ordinary
exact rerun that observes the accepted row in its initial ledger read takes no
installation lock; only the commit-between-reads race can reach the locked
exact no-op.

With continued ledger absence, the installer row-locks with `SELECT ... FOR
UPDATE` the runtime-mode singleton, M4 publication-head singleton, M5
publication-head singleton when present, and activation singleton when
present, in that order. A missing M5 head or activation row is protected by
the complete table-lock barrier, so absence cannot turn into presence during
installation. The installer then checks the no-typed-history predicate and all
Section-11 preconditions. Any changed singleton or forbidden row aborts before
DDL.

D25 patch/contribution insertion participates in the existing mutator's one
D24 transition timing anchor; it MUST NOT designate or append a second timing
point.

## 11. Bootstrap, upgrade, and no-guess barrier

The installer first applies Section 3's ledger-first rerun/conflict decision.
Only on a first installation, after validating all five literal accepted-016
fields and acquiring the Section-10 locks, migration 017 MUST reject any
pre-017 `groundloop_m5_runtime_epoch` or `groundloop_m5_update` row, even if a
runtime row is terminal and even if it has zero attempts. Exact historical D25
patches, matching work, image transitions, and representative reads cannot be
reconstructed from terminal semantic state. It MUST NOT guess that history or
silently mark it zero.

This first implementation supports:

1. a populated `v1_only` database with arbitrary accepted M1--M4 and
   migration-014 semantic data but no typed runtime epoch/update; and
2. an already activated database with equal M4/M5 heads and no typed runtime
   epoch/update.

For `v1_only`, migration 017 installs empty physical current/working/image,
patch, contribution, and accumulator tables. M5 activation MUST require exact
accepted 014, 015, all five corrected-016 literals, and 017 ledger rows, then
bootstrap the current physical rows and current-image header in the same
activation transaction as semantic state and the M5 head.

For `m5_active` with no typed runtime history, migration 017 MUST reconstruct
the current image and every expected comparison value from the strict M5 head
using only these construction inputs:

Before construction it MUST prove `mode=m5_active`, exactly one activation row,
zero `groundloop_m5_runtime_epoch` rows, and zero `groundloop_m5_update` rows.
Let `E` be the locked M5 head epoch and `R` its sealed revision. It MUST prove
`activation.base_m4_epoch_id = M4_head.epoch_id = M5_head.epoch_id = E`, that
epoch `E` satisfies the frozen sealed predicate, and
`M5_head.sealed_revision = groundloop_epoch.revision = R`. Exactly one
decision-policy interval MUST cover `E`, and the current currency map MUST
equal the unclosed published-currency map at `E`. A missing, extra, or
overlapping row aborts before DDL.

- the locked runtime-mode, activation, equal M4/M5 head epoch IDs, M5
  `sealed_revision`, and sealed `groundloop_epoch` row;
- group-family, group-version, requirement-version, dense ordinal, lineage,
  and lifecycle rows evaluated at that head;
- active document/chunk versions and their exact canonical normalized text
  hashes;
- the one exact current decision-policy row;
- immutable eligible requirement-subject and claim-subject observations plus
  the exact `groundloop_observation_currency` map and strict current
  `groundloop_published_observation_currency` rows; and
- immutable `groundloop_claim` ownership/`required` rows and their referenced
  `groundloop_answer_version` rows, which define the complete group-to-claim,
  claim-to-answer, and required-claim aggregation inputs.

This construction-input list is closed. Existing M4 and M5 materialized or
published requirement/group/claim/answer states, `groundloop_claim_certificate`,
M5 group/claim certificate bindings, and certificate artifacts are comparison
targets only; none may seed an expected count, status, selected representative,
or digest. No M5-D25 image, observation, edge, mask, Hall, patch, contribution,
or accumulator row is an allowed expected-state input. All construction and
target rows are read under the same locked strict head. An absent, mixed-head,
duplicate-current, or certificate/state-inconsistent target aborts before DDL
or backfill writes.

This installation/activation bootstrap may use full base scans and the bounded
initializer because it is explicitly outside measured event latency. Before
commit it MUST establish exactly one current decision-policy identity, prove it
agrees with every applicable current semantic state and certificate, and
compare all reconstructed observation memberships, edge refcounts, hash masks,
Hall arrays/scalars, requirement/group/claim/answer states, current bindings,
and selected certificate rows against the strict published snapshot. The
current-image epoch MUST equal both head epoch IDs, and its revision MUST equal
the M5 `sealed_revision` and sealed epoch revision. Any mismatch aborts the
whole migration/activation.

Strict direct-M4 claim/answer states and direct certificates are first
reconstructed from the claim observations/currency and ownership inputs above,
not copied from their current rows. Combined claim state is reconstructed from
that expected direct claim state plus independently derived complete-group
IDs. Combined answer state is then reconstructed only from the closed claim-
to-answer/required-claim input. A direct-only, group-only, refuted, conflicted,
optional-claim, or multi-answer fixture MUST therefore be derivable without
reading a stored M4 or M5 claim/answer state as an input. Direct and M5
certificate targets are likewise validated from their immutable source
observations/groups before their stored identities are compared.

Bootstrap is not a typed semantic transition: it MUST create no D25 patch,
contribution, accumulator, synthetic epoch, work, or timing anchor. Its full
scans/initializer are installation-only code and MUST NOT be callable from the
measured open, completion, reconnect, failure, or seal routes.

Every future activation, typed open, and nonterminal typed resume MUST verify
the exact accepted migration-017 ledger row before event-ID/epoch consumption
or any external work. An already activated database without 017 may perform
read-only audit/terminal replay but cannot open or resume typed mutation until
the first-install backfill succeeds. Never-activated public M4-v1 behavior
remains unchanged.

Supporting a database that already executed typed events without D25 requires
a separate numbered, evidence-backed conversion decision. It is not part of
this candidate.

## 12. D24/D25 counter ownership

Corrected M5-D24 MUST remain sole owner of:

- actual external dispatch, retry, expiry, takeover, and artifact-reuse
  accounting;
- forward, reverse, fallback, verifier, embedding, and other model-call/token
  counters;
- discovery, admission, observation-artifact, cancellation, and late-result
  counters;
- actual requirement/group/claim/answer/certificate/public-delta SQL write
  counts in `M5RuntimeWork`;
- runtime bytes hashed/serialized;
- all call/event timing and PostgreSQL physical counters; and
- terminal copying and replay of `M5RuntimeWork`.

M5-D25 MUST remain sole owner of the exact logical matching/overlay counters
in Section 4.6. The store computes them from the jointly validated physical
and logical Section-4.7 patch plus actual ordered operations; workers MUST NOT
supply or increment them.

One transaction may update both surfaces because they measure different
things. For example:

- D25 `groups_touched` counts logical M5-T2 group keys;
- D24 `group_state_write_count` counts actual persisted state writes;
- D25 `output_bytes` is the frozen logical overlay image size; and
- D24 `bytes_serialized` is runtime serialization work.

These paired values MUST be labelled separately and MUST NOT be summed as if
disjoint. Oracle/audit work and time belongs to neither measured accumulator
and is reported separately. A D25 patch/contribution is applied inside the
one existing D24 mutator and never creates a second D24 work contribution,
transition timing anchor, or event logical-result field.

M5-D25 remains strictly conditional on the final accepted D24 contract and
implemented migration-016 ledger bytes. Section 3's five exact literals and
the mandatory Section-15 same-byte audits are the hard barrier; D25 does not
reopen or silently revise D24 dispatch, reuse, fallback, direct-recovery,
timing, or no-guess upgrade semantics.

## 13. Falsifying acceptance matrix

Every row is mandatory. A skipped, unavailable, no-match, or environment-
failed test remains non-PASS.

1. **Reconnect without any cache.** A fresh store/process completes and seals
   after the prior process is gone while every `M5IncrementalOverlay`
   constructor, repository hydration path, Python recomputation, SQL
   Hall/assignment oracle, runtime-book reconstruction, aggregate job scan,
   and injected process-cache access raises.
2. **Physical-image policy.** Activation, ordinary open, nonzero-flip policy,
   and zero-flip policy each bind the exact current/working decision policy;
   a mismatched policy/header/base point fails before a physical read or write.
3. **Zero to mask.** A first SUPPORT edge creates one observation row, edge
   refcount one, the exact bit in `M[h]`, and the exact Hall transition.
4. **Mask to zero.** Removing the last observation removes the edge bit,
   stores working zero tombstones, and updates every affected Hall subset.
5. **Mask to different mask.** Coalesced changes across ordinals perform one
   `m1 -> m2` transition for the hash.
6. **Net-equal transition.** A supersession whose final mask equals its prior
   mask performs zero Hall work and retains exact final provenance.
7. **Positive multiplicity.** A `1 -> 2` or nonselected `2 -> 1` change updates
   observation/refcount state but performs zero mask/Hall work.
8. **Selected provenance repair.** Removing the selected observation at
   `2 -> 1` selects the least remaining observation, changes the certificate
   digest/binding as required, and leaves Hall/group status unchanged.
9. **Alternating rebuild.** Losing a selected edge while another covering
   matching survives rebuilds from capped C-ordered representatives without
   scanning total witness degree.
10. **No-zero-crossing Hall failure.** The frozen `r3,c` deletion makes the
    group incomplete despite every requirement remaining satisfied.
11. **Local Hall corruption.** Mutating any histogram, neighbour, deficiency,
    maximum, matching-size, or distinct-hash field is rejected by the bounded
    row validator or detected by the post-seal independent audit.
12. **Representative shadow/order/cardinality.** Non-ASCII and prefix-
    adversarial IDs/hashes prove `COLLATE "C"` ordering; resolved point APIs
    distinguish physical absence, current values, working values, and every
    working tombstone before the filtered effective-value API collapses an
    inactive value; any working tombstone or different-mask row hides current;
    every mask returns exactly `min(C[m],r_g)` hashes; and a selected positive
    edge cannot return NULL provenance.
13. **Patch one-field identity.** Mutating every source, point, policy, group
    shape, physical before/after field, logical state/artifact/binding field,
    logical-output byte, or one of all 37 work counters changes the applicable
    child, patch, work, and contribution digest. Golden vectors cover all nine
    exact output-kind strings, group-then-claim first-touch binding sequence,
    close/open laws, physical-audit family rank, typed audit keys, and exact
    mismatch preimages.
14. **Patch/contribution replay.** Exact structural, requirement, and direct
    source replay point-reads one retained artifact/contribution and performs
    zero writes; same source or resulting revision with different patch/work/
    before image conflicts atomically.
15. **Semantic-transition coverage.** Structural open creates one contribution
    even when every change sequence is empty and then charges the exact
    71-byte empty logical output; active requirement/direct transitions each
    create one; intervening acquisition/root/barrier/cancellation/inactive/
    audit/failure/seal revisions do not alter D25 work, and seal does not
    require D25 `updated_revision` to equal the runtime revision.
16. **Completion crash matrix.** Injection after each image, observation,
    edge, mask, Hall, semantic state, certificate, combined-state, patch
    artifact, D25 contribution, D24 accumulator, D25 accumulator, job, and
    revision write exposes only the complete before or complete after image.
17. **Failure isolation.** A failed epoch retains its working image,
    tombstones, patch artifacts, contributions, and counters while current,
    materialized, published, certificates, and both heads remain byte-
    identical.
18. **Next-epoch isolation.** A valid retry after failure reads the sealed
    current image and ignores every failed-epoch overlay row while retaining
    that failed history for audit.
19. **Seal atomicity.** Injection across physical-image/row promotion,
    semantic/certificate/M4 promotion, both head advances, result insertion,
    and epoch seal exposes either the entire old or entire new publication.
20. **Seal replay.** Two seals return one stored result and promote the
    physical image once without changing retained patch/contribution rows.
21. **Concurrent completion.** Same-edge and different-edge concurrent
    completions serialize through epoch CAS with no lost refcount, mask bit,
    certificate update, patch, contribution, or counter.
22. **Structural register.** Registration creates exactly one empty Hall row,
    dense group shape, semantic state, image/patch/contribution, and no model
    call; failure/replay is atomic.
23. **Structural replace.** Failure leaves the predecessor current;
    successful seal removes all predecessor physical rows and installs the
    exact empty successor Hall/group shape with one coalesced patch.
24. **Structural retire.** Retirement enumerates only the named group,
    tombstones all and only its physical rows, preserves current on failure,
    and publishes no successor.
25. **Document delete/replace.** Exact frozen withdrawal observation IDs are
    point-removed without rediscovery, a hidden reserve, or model work;
    changed withdrawal/predecessor bytes conflict on replay.
26. **Document insert and explicit observation.** A no-membership document
    insert still records the exact working image and byte-total structural
    patch/contribution without inventing physical work. Standalone claim
    `ObserveEvent` proves its direct-M4 and combined-v2 effects coalesce into
    the sole revision-1 structural contribution. Parameterized
    `ObserveRequirementEvent` SUPPORT/REFUTE/NEUTRAL, active canonical,
    active noncanonical, inactive requirement, inactive chunk, first-holder,
    supersession, selected-provenance, exact replay, and conflicting-payload
    cases prove one rootless revision-1 structural contribution, the exact
    eligible/ineligible currency and matching effects, and zero model calls.
27. **Policy nonzero flips.** Ordered candidate probes produce every exact
    SUPPORT addition/removal, coalesced mask/Hall changes, policy-bound
    certificates, image identity, patch, and counters.
28. **Policy zero flips.** Empty and nonempty no-flip candidate ranges record
    exact ordered probes, image-policy advance, and required complete-group/
    typed-claim rebindings with zero mask/Hall transitions.
29. **Counter separation.** D24 external/physical and D25 logical counters
    survive reconnect/failure; replay increments neither; the report never
    merges estimands; D25 creates no second D24 timing anchor.
30. **Migration literal prerequisite.** Freeze/build fails while any accepted-
    016 placeholder remains; first install rejects a missing or one-field-
    mismatched accepted-016 ledger row before DDL.
31. **Migration paths.** Fresh, populated-v1, activated-without-history,
    same-ID hash conflict, live/pre-D25-terminal history rejection, and every
    mid-DDL/backfill injection are atomic. Exact 017 rerun checks its ledger
    first and is a no-op after later typed history exists; activation/open/
    nonterminal resume reject a missing 017 ledger before consumption. Static/
    spy and live both-order tests assert the complete Section-10 table tuple,
    `ACCESS EXCLUSIVE MODE NOWAIT`, singleton-row order, and absence protection
    against public-v1 open, activation, and pre-017 typed open/resume. Two
    same-byte installers, two conflicting installers, a commit between the
    initial ledger read and first lock, partial-prefix lock failure/release,
    and successful new-transaction retry prove the mandatory post-lock ledger
    recheck and prohibit waiting or retry inside the transaction. Ambient,
    nested/savepoint, read-only, REPEATABLE READ, and SERIALIZABLE installer
    invocations are rejected before the initial ledger decision; the accepted
    path proves one top-level read-write READ COMMITTED transaction.
32. **Activation bootstrap.** Activation creates image policy plus exact
    observation/refcount/mask/Hall state equal to semantic states/current
    bindings/certificates at the existing M4 head, without a synthetic epoch,
    D25 patch, contribution, accumulator, work, or timing anchor.
33. **Activated no-history backfill.** Full reconstruction is confined to
    migration, proves one policy and both equal heads, and rejects every seeded
    physical/semantic/certificate mismatch without partial DDL or ledger.
34. **Point plans.** `EXPLAIN` proves primary-key, anti-join, epoch-prefix,
    mask/edge, affected-group, and seal-promotion indexes; no oracle relation
    or full base scan appears in a measured plan.
35. **Oracle and physical-audit separation.** Python and SQL expected-state
    oracle definitions contain no D25 relation name and return byte-identical
    Section-6.1 expected projections. The separate actual-image adapter plus
    provenance pass detects each seeded current-image policy, working-image
    base/policy/status/revision, installed-coordinate, observation, refcount,
    mask, Hall, patch, contribution, retained-working, or accumulator
    corruption. Missing/extra rows and every accumulator field are covered;
    a valid unique outer key with a malformed/noncanonical payload returns the
    exact no-actual-projection `malformed_payload` artifact with the frozen
    false/absent/empty skipped-provenance fields, while an
    undecodable/duplicate outer key or invalid current-image header returns a
    typed no-artifact invalid audit;
    consistently re-digesting a corrupted accumulator still fails the
    independent contribution sum and pairwise provenance digest comparison;
    ordinary three-oracle semantic equality independently detects seeded
    requirement/group/claim/answer-state and certificate divergence. Giving a
    D25 value to an expected-state oracle fails the test.
36. **Canonical task boundary.** A current eligible noncanonical SUPPORT
    observation remains auditable but creates no physical membership, patch
    edge, or matching work.
37. **Inactive and audit-only return.** Inactive completion and expired/
    cancelled/failed late output change no D25 image, row, patch,
    contribution, or counter.
38. **Direct logical-only transition.** A direct transition with no Hall
    change persists a physical-empty but byte-total logical patch and exact
    state/certificate/output work without a process overlay.
39. **Direct-only/v1 regression.** A zero-group typed event and every never-
    activated M4-v1 route preserve frozen M4 identities/behavior; new
    relations remain empty or physically inert as appropriate.
40. **All-relation raw-DML guard.** For every D25 current, working, image,
    patch, contribution, and accumulator relation, an unauthorized raw
    `INSERT`, `UPDATE`, and `DELETE` each fail before a row changes. Checked
    runtime authorization succeeds only after the exact epoch/revision CAS;
    structural open authorizes the already-inserted new epoch at runtime point
    `(epoch_id,1)` while retaining the sealed predecessor as patch before-
    point, and later transitions authorize `N -> N+1`;
    checked activation authorization succeeds only after the exact ledger/
    head/policy/no-live-epoch checks; first-install backfill commits with all
    guards enabled. Boolean-only and wrong mode/epoch/expected revision/
    resulting revision/source/relation/operation contexts are rejected; direct
    context setting and guard disabling are absent from every ordinary
    application path. Rollback and reconnect show no partial physical,
    logical, ledger, or authorization state.

## 14. Claim boundary

If accepted and implemented, M5-D25 would establish failure-atomic,
reconnectable PostgreSQL maintenance of the frozen bounded Hall implementation
state relative to stored versioned observations and decision policy. It would
support measured point/affected-set execution without a process cache or
inline full oracle.

It would not establish objective truth, exactly-once model-provider execution,
semantic retrieval completeness, worst-case sublinear updates, constant
physical database work, general dynamic-matching novelty, cryptographic
privacy, security, or superiority over DBSP, F-IVM, CROWN, or another named
system. Dense policy and structural batches, logical output size, PostgreSQL
indexes/planning/locks/WAL/I/O, and neural work remain explicit. Independent
Python and SQL audits remain mandatory after every measured history seal.

Fresh independent human adjudication remains M6 debt and is not supplied by a
controlled or retrospective M5 runtime history.

## 15. Candidate verdict

**Decision status: remediated contract candidate awaiting two independent
same-byte adversarial reviews; not frozen and not implementation authority.**

This candidate derives from protected draft SHA-256
`167d1e7df5a720041fe0ff51879d08357f0dfbe3a7781ffaa08c0d958a47aa94`.
It pins the accepted migration-016 tuple and remediates the pre-activation
caller/store, physical encoding, lock-order, bootstrap-input, physical-audit,
and head-revision defects. It further freezes lossless raw point APIs, exact
logical-output kinds/binding order and audit encodings, standalone claim and
inactive requirement observation behavior, deterministic seal provenance,
nonzero empty-output bytes, fail-fast migration concurrency with a post-lock
ledger recheck, and complete working-image/accumulator reconciliation. Those
edits are proposals until the reviews and authority freeze below complete.

The production runtime remains NO-GO for durable matching completion,
reconnect, measured seal, and maintained M5.5 evidence until:

1. the exact Section-3 migration-016 tuple remains independently reproducible
   on the candidate base with no migration/installer drift;
2. this complete candidate receives two `GO` verdicts on one exact SHA-256:
   one semantic/digest/counter/oracle audit and one PostgreSQL/migration/lock/
   replay audit, each with zero unresolved P0/P1;
3. after both verdicts, the byte-identical amendment, numbered M5-D25
   decision, runtime-addendum revision, acceptance matrix, implementation
   plan, status, roadmap, and path-exclusive ownership amendments are frozen
   in a separate coordinator tranche; and
4. only after that authority reaches main may a separate activation authorize
   migration 017 and the cursor-local store to implement and pass every
   falsifier above.

Any candidate byte change invalidates both review verdicts. Migration 017,
source implementation, database mutation, runtime activation, M5.4-05 through
M5.4-09 promotion, or an M5.5 maintained-runtime claim before those gates is
unauthorized.
