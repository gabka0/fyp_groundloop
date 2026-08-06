# GroundLoop M5 Bounded Evidence-Group Design Freeze

Status: frozen M5.0 contract, amended through accepted M5-D24-C1;
implementation evidence for M5-D24 and M5-D24-C1 remains pending

Date: 2026-08-02; M5-D21 through M5-D24-C1 amendments 2026-08-06

Authority: this document specializes `docs/technical_design.md` v0.2 for M5.
It preserves original decisions D-1 through D-20 except where the earlier
pseudocode is mathematically inconsistent with its own stated system-of-
distinct-representatives semantics. Those corrections and the later runtime
decisions M5-D21 through M5-D24-C1 are recorded in the decision log and frozen
here. The byte-total M5-D24 specialization is authoritative at
`docs/workstreams/m5_runtime_contract/RECOVERY_WORK_AMENDMENT.md`.
`docs/workstreams/m5_runtime_contract/EXECUTION_DISPOSITION_RECEIPT_CORRECTION.md`
is its accepted execution-disposition and return-receipt correction; its
independently accepted pre-freeze content SHA-256 is
`741ce0de897099164eb877684bc12209f347eb920185be5ed4c3c3d5395bf25a`.

M5 implementation begins only after the M5.0 *contract* gate passes. Later
implementation-evidence cells in the acceptance matrix remain `PENDING` until
their M5.1--M5.6 gates execute.

## 1. Verdict and bounded scope

M5 implements versioned, support-only evidence groups for already registered
claims. A group is a conjunction of one to eight evidence requirements. A
claim is supported when either:

1. it has at least one direct, distinct-content supporting witness; or
2. at least one active evidence group has a distinct supporting witness for
   every active requirement.

Multiple groups for the same claim are alternatives (logical OR). Requirements
inside one group are conjunctive (logical AND). Witnesses for one requirement
are alternatives (logical OR). This is a bounded OR-AND-OR structure, not a
recursive reasoning graph.

M5 CORE includes:

- immutable version identities for groups and requirements, with one
  group-validity sidecar from which requirement activity is derived;
- requirement-subject score observations;
- exact distinct-content witness edges;
- exact bounded bipartite matching for group completeness;
- incremental affected-group maintenance and group-to-claim-to-answer
  propagation;
- independent Python and PostgreSQL full-recomputation oracles;
- subject-aware dynamic jobs and publication compatible with M4;
- gold/controlled evaluation of invalidation behavior.

M5 excludes:

- conjunctive or group-level refutation;
- recursive evidence graphs;
- model-proposed group quality as a headline result;
- online model-weight updates;
- a claim of a novel general dynamic-matching algorithm;
- a claim that the verifier or group constructor maintains objective truth;
- a representative natural-history utility claim, which remains M6 debt.

## 2. Exact and empirical boundaries

The exact M5 contract starts after groups, requirements, score observations,
currency, validity intervals, and a decision policy are fixed:

```text
M5 incremental state
  == independent Python full recomputation
  == independent SQL full recomputation
```

Equality covers requirement witness sets, matching size, group completeness,
matching-certificate validity, complete-group counts, complete claim and answer
states, and public status deltas.

The following remain empirical:

- whether a requirement decomposition is semantically appropriate;
- whether a model-proposed group is sufficient or minimal;
- whether retrieval finds relevant requirement/chunk pairs;
- whether the verifier scores a pair correctly;
- whether group-aware maintenance reduces false invalidation on a population;
- whether selective processing saves useful work at adequate recall.

Gold labels are evaluation inputs, not neural observations secretly promoted
to truth. Model outputs and gold annotations are stored and reported
separately.

## 3. Version identities and temporal semantics

### M5-D1 -- stable family and immutable version identity

`group_family_id` identifies one alternative support route across time.
`group_version_id` and `requirement_version_id` identify immutable versions.
Replacement never reuses a version identifier. The immutable owner belongs to
the family, not to each version:

```text
EvidenceGroupFamily(
  group_family_id, claim_id, created_epoch
)

EvidenceGroupVersion(
  group_version_id, group_family_id, group_type, construction_kind,
  construction_source_id,
  constructor_model_id?, constructor_model_version?, constructor_prompt_version?,
  supersedes_group_version_id?, semantic_structure_hash, record_payload_hash
)

EvidenceRequirementVersion(
  requirement_version_id, group_version_id, ordinal, requirement_text,
  requirement_text_hash, constructor_model_id?, constructor_model_version?,
  constructor_prompt_version?, supersedes_requirement_version_id?
)

EvidenceGroupVersionValidity(
  group_version_id, valid_from_epoch, valid_to_epoch?
)
```

Group and requirement semantic records are immutable. Validity is a temporal
sidecar; requirement activity is the parent group interval and is not copied
into a second independently mutable interval. Closing a version therefore
never changes `record_payload_hash`.

`EvidenceGroupFamily.claim_id` is immutable. Registration creates a new
family; a retired family cannot be reopened. Every noninitial group version
must supersede the version closed by the same replacement event, in the same
family, with `old.valid_to_epoch = new.valid_from_epoch`. The predecessor link
is unique, so forks are impossible; equal predecessor-close/successor-open
boundaries and the same-family constraint prohibit cycles and skipped family
versions. Epoch IDs need not be globally consecutive because unrelated events
may occur between family replacements. A
range-exclusion constraint prohibits historical validity overlap within one
family.

These are published-history invariants. In the M4 physical path a pending
replacement is represented by a STAGED successor plus an epoch-local
deactivation overlay; it does not mutate the predecessor's published interval
until seal. A FAILED staged successor is historical execution evidence, not a
published successor, and does not consume the published-lineage uniqueness
slot.

A requirement predecessor, when present, must belong to the immediately
preceding group version in that family and is unique. Requirement ordinals are
exactly the dense set `0..r_g-1`; negative, one-based, duplicated, or gapped
ordinals are invalid.

Two hashes have separate purposes:

```text
semantic_structure_hash = stable_m5_digest(
  "m5-semantic-structure-v1", *ENUM(group_type),
  *SEQ(sorted requirement_text_hash values encoded with HASH)
)

record_payload_hash = the byte-exact `m5-group-record-v1` recipe below
```

`stable_m5_digest` uses SHA-256 over ordered UTF-8 fields, each preceded by an
unsigned eight-byte big-endian byte length. Requirement text uses normalization
v1: strip, collapse whitespace runs to one ASCII space, preserve all other
Unicode code points, then SHA-256. For cross-language stability, whitespace in
normalization v1 is exactly the following 29 code points, not a runtime- or
locale-dependent regex class:

```text
U+0009..U+000D, U+001C..U+0020, U+0085, U+00A0, U+1680,
U+2000..U+200A, U+2028, U+2029, U+202F, U+205F, U+3000
```

Leading and trailing runs are removed; every internal nonempty run becomes
U+0020. Empty normalized text is invalid. Python and PostgreSQL implement this
exact code-point predicate and share golden vectors; neither implementation's
ambient Unicode-regex behavior defines the contract.
`EvidenceRequirementVersion.requirement_text` stores that normalized value,
not the raw source rendering; optional raw/source offsets live only in
provenance/evaluation records. Its stored hash must equal SHA-256 of the stored
UTF-8 text.

All M5 v2 identities use the following typed field expansion before calling
`stable_m5_digest`; no implementation may use `repr`, ordinary JSON defaults,
delimiter concatenation, or an implementation-specific null value:

```text
TEXT(x)   -> ("text", x)
NULL      -> ("null",)
INT(n)    -> ("int", canonical base-10 n with no leading zero)
BOOL(b)   -> ("bool", "1" if b else "0")
ENUM(x)   -> ("enum", x)
HASH(x)   -> ("sha256", 64 lowercase hexadecimal characters)
F64(x)    -> ("f64", the 16 lowercase hexadecimal digits of the
              big-endian IEEE-754 binary64 bit pattern)
SEQ(xs)   -> ("sequence", *INT(len(xs)), then each already typed item)
OPTION(x) -> NULL when absent, otherwise the typed expansion of x
```

The first digest field is always the literal domain tag shown below. All IDs
and present text fields are nonempty. Sequences are in their stated canonical
order. This typed expansion plus eight-byte length framing is the complete
null/empty/order contract.

For a group record, requirements are serialized in ordinal order. The exact
record digest is:

```text
stable_m5_digest(
  "m5-group-record-v1",
  *TEXT(owner_claim_id), *TEXT(group_version_id), *TEXT(group_family_id),
  *ENUM(group_type),
  *ENUM(construction_kind), *TEXT(construction_source_id),
  *OPTION(TEXT(constructor_model_id)),
  *OPTION(TEXT(constructor_model_version)),
  *OPTION(TEXT(constructor_prompt_version)),
  *OPTION(TEXT(supersedes_group_version_id)),
  *HASH(semantic_structure_hash),
  *SEQ(for each requirement in ordinal order:
       SEQ((TEXT(requirement_version_id), INT(ordinal),
            TEXT(normalized_requirement_text), HASH(requirement_text_hash),
            OPTION(TEXT(constructor_model_id)),
            OPTION(TEXT(constructor_model_version)),
            OPTION(TEXT(constructor_prompt_version)),
            OPTION(TEXT(supersedes_requirement_version_id)))))
)
```

`semantic_structure_hash` uses the same typed expansion: ENUM group type then
a SEQ of requirement-text HASH values sorted lexicographically. This replaces
the informal starred notation above without changing its commutative meaning.

Conjunction is commutative, so `semantic_structure_hash` deliberately excludes
ordinal, version IDs, lineage, and constructor provenance. A unique active
`(claim_id, semantic_structure_hash)` constraint prevents a reordered or
reconstructed duplicate from inflating `CompleteGroupCount`.

### M5-D2 -- whole-group lifecycle

- Registration appends one group and its complete requirement set atomically.
- Replacement closes the old group-validity interval, which makes every old
  requirement inactive, and appends the complete successor set in one event
  and epoch.
- Retirement closes the group-validity interval, which makes all of its
  requirements inactive, in one event.
- Requirements cannot be added, removed, moved, reopened, or edited in place.
- In CORE requirement activity is derived from its parent group validity
  sidecar; no independently writable requirement interval exists.
- Empty groups, groups with more than eight requirements, nondense ordinals,
  and duplicate normalized requirement text within a group are rejected.
- At most one active successor exists for a lineage.

The allowed group type is `support_conjunction`. Construction kind is one of
`gold`, `controlled`, or `model_proposed`. Gold and controlled groups are the
only kinds admitted to the primary M5 acceptance result. Dataset origin such
as WiCE is stored in `construction_source_id`; `wice_controlled` is not a
fourth construction kind.

`construction_source_id` is nonempty for every kind and identifies the source
annotation/controlled-fixture manifest or the model-input manifest. For
`gold` and `controlled`, all three constructor-model fields are NULL. For
`model_proposed`, all three constructor-model fields are nonempty. Requirement
constructor fields must exactly equal their parent group's three fields. No
kind may smuggle model provenance into `construction_source_id` while leaving
required model fields NULL.

M5 structural event payloads are also total:

```text
RegisterGroup payload = stable_m5_digest(
  "m5-register-group-event-v1", *TEXT(group_family_id), *TEXT(claim_id),
  *HASH(record_payload_hash))
ReplaceGroup payload = stable_m5_digest(
  "m5-replace-group-event-v1", *TEXT(old_group_version_id),
  *HASH(successor_record_payload_hash))
RetireGroup payload = stable_m5_digest(
  "m5-retire-group-event-v1", *TEXT(group_version_id))
ObserveRequirement payload = stable_m5_digest(
  "m5-observe-requirement-event-v1", *TEXT(observation_id),
  *ENUM(subject_kind), *TEXT(subject_id), *TEXT(chunk_version_id),
  *TEXT(task_type), *F64(support_score), *F64(refute_score),
  *F64(neutral_score), *TEXT(model_id), *TEXT(model_version),
  *TEXT(prompt_version), *HASH(input_hash))
```

The outer event ID is the idempotency key and is not part of these payload
digests. An exact replay compares the stored payload digest; the same event ID
with a different digest conflicts. Epoch/validity sidecars are assigned by the
successful transaction and are deliberately absent, so replay bytes never
depend on the current head.

### M5-D3 -- late and historical observations

An observation result may be archived for an inactive chunk or inactive
requirement. It remains auditable but is marked `eligible_for_currency=false`,
does not supersede the existing currency holder, and contributes nothing. An
eligible requirement observation is active only when all of these are true:

1. it is the current holder of its observation currency key;
2. its chunk version is active;
3. its requirement version is active;
4. its parent group version is active; and
5. the current decision policy derives SUPPORT.

New requirement versions use new identifiers; old observations never transfer
implicitly across group replacement. Exact reuse, if added later, requires a
separate provenance-bearing event and is not M5 CORE.

## 4. Semantic subjects and witnesses

### M5-D4 -- referentially safe subjects

The observation key remains:

```text
(subject_kind, subject_id, chunk_version_id, task_type)
```

`subject_kind` is CLAIM or REQUIREMENT; a requirement subject ID is its
immutable `requirement_version_id`. PostgreSQL uses a typed semantic-
subject registry keyed by `(subject_kind, subject_id)`, backfilled for existing
claims and populated transactionally for every future claim and requirement.
The registry is append-only and its kind cannot change. The same raw string ID
may occur once in each typed namespace without collision.

Observation integrity uses a composite foreign key to the registry *and* a
deferred subtype-validation trigger: CLAIM registry rows must resolve to an
actual claim, and REQUIREMENT rows must resolve to an actual requirement
version. Claim and requirement insert triggers maintain the registry. A
registry row alone is therefore not allowed to launder an orphan or a
wrong-kind identifier. M5 must not merely drop the M2 claim foreign key or its
claim-only check.

Currentness is historical, not only a mutable pointer. Both the in-memory M5
reference repository and PostgreSQL retain half-open observation-currency
intervals. Certificate validation at an `(epoch_id, revision)` snapshot must
prove that each selected observation was the holder then, in addition to
checking chunk/requirement/group validity and the bound decision policy.

The canonical M5 requirement task type is `verify_requirement_v1`. A
requirement observation with another task type cannot enter the witness view,
even if its score would otherwise derive SUPPORT. Model, prompt, calibration,
input, and decision-policy identities remain explicit provenance.

### M5-D5 -- RequirementWitness is derived

There is no writable `RequirementWitness` base relation in M5 CORE. It is a
derived, policy-relative view over current requirement-subject observations:

```text
ActiveRequirementWitness(r, o, h) :=
  o.subject = REQUIREMENT r
  and o is current
  and chunk(o) is active with normalized text_hash h
  and r and group(r) are active
  and decide(o, current_policy) = SUPPORT
```

This resolves the undefined `witness_policy_version` in the original logical
schema and prevents a writable link from disagreeing with the observation's
typed subject. Every active requirement-subject SUPPORT observation is an
eligible witness. Candidate/admission provenance remains in the dynamic job
relations, not in a second semantic link.

Requirement REFUTE means that the chunk does not satisfy that requirement. It
does not refute the parent claim. Parent-claim refutation remains direct
claim-subject REFUTE evidence only.

### M5-D19 -- canonical requirement task admission

Only current observations whose task type is exactly
`verify_requirement_v1` can enter `ActiveRequirementWitness`. Other typed
requirement observations remain auditable but inert for M5 witness state.

### M5-D11 -- requirement judgments do not refute the parent claim

A requirement REFUTE or NEUTRAL decision only means that the requirement has
no supporting edge from that observation. It never increments the parent
claim's direct `refute_count`. Parent refutation is derived exclusively from
current direct claim-subject REFUTE observations.

## 5. Correct group-completeness semantics

### M5-D6 -- exact distinct-representative completeness

For one active group `g`, define a bipartite graph:

```text
left vertices R_g  = active requirements of g
right vertices H_g = distinct normalized text_hash values
edge (r, h)        = at least one ActiveRequirementWitness(r, o, h)
```

Let `nu(g)` be the maximum matching cardinality. The only completeness rule is:

```text
GroupComplete(g) := |R_g| > 0 and nu(g) = |R_g|
```

The earlier global-union-size condition is not sufficient. For example:

```text
r1 -> {a, b}
r2 -> {a, b}
r3 -> {a, b}
r4 -> {c, d}
```

The global union has four hashes for four requirements, yet `r1`, `r2`, and
`r3` collectively have only two neighbors. Hall's condition fails and the
group is incomplete.

The maintained states are:

```text
RequirementWitnessHashes(r)
RequirementWitnessCount(r) = |RequirementWitnessHashes(r)|
RequirementSatisfied(r)    = RequirementWitnessCount(r) > 0

GroupRequirementCount(g)   = |R_g|
GroupSatisfiedCount(g)     = count of individually satisfied requirements
GroupMatchingSize(g)       = nu(g)
GroupComplete(g)           = GroupMatchingSize(g) = GroupRequirementCount(g) > 0

CompleteGroupIds(c)        = active complete groups owned by c
CompleteGroupCount(c)      = |CompleteGroupIds(c)|
```

`GroupSatisfiedCount` is diagnostic state. It does not determine completeness.

## 6. Matching and certificates

### M5-D7 -- every distinct edge crossing is material

Every net `(requirement_version_id, text_hash)` refcount crossing between zero
and positive changes one bit of one group/hash mask. Positive-to-positive
multiplicity changes do not change Hall state, but can repair selected
certificate provenance. Requirement-satisfaction zero crossings are only a
diagnostic subset of the required triggers.

### M5-D8 -- bounded Hall-mask maintenance

For each group, requirements occupy fixed ordinal bits `0..r_g-1`. For every
distinct active text hash `h`, maintain an adjacency mask `M[h]` whose set bits
are the requirements witnessed by `h`. Maintain:

```text
C[m]    = number of distinct hashes with exact nonzero mask m
N[S]    = number of hashes adjacent to at least one requirement in subset S
d[S]    = popcount(S) - N[S]
delta   = max(0, max_S d[S])
```

By the deficiency form of Hall's theorem:

```text
maximum_matching_size = r_g - delta
GroupComplete          = r_g > 0 and delta = 0
```

When a coalesced edge update changes one hash mask from `m_old` to `m_new`, the
kernel updates `C`, all `N[S]` whose intersection status changed, and the
deficiency maximum over at most `2^r_g - 1` nonempty subsets. With `r_g <= 8`,
this is at most 255 subset entries per group/hash transition and is independent
of the total witness degree of that group.

Initialization computes `N[S]` from `C` with a complement subset-zeta
transform in `O(r_g 2^r_g)`. A deliberately simpler affected-group
augmenting-path recomputation is retained as a correctness and performance
baseline. M5 does not claim new general dynamic-matching theory; its optimized
operator exploits the frozen small left side.

### M5-D9 -- matching certificate

A complete group certificate is an immutable content artifact with exactly one
row per requirement. Working history points to artifacts at an exact semantic-
epoch revision; a certificate artifact itself has no mutable validity fields:

```text
GroupMatchingCertificateArtifact(
  certificate_digest,
  decision_policy_version, certificate_version, group_version_id,
  requirement_count
)

GroupMatchingCertificateArtifactRow(
  certificate_digest, requirement_ordinal,
  requirement_version_id, text_hash, selected_observation_id
)

WorkingGroupCertificateBinding(
  epoch_id, group_version_id, valid_from_revision, valid_to_revision?,
  certificate_digest
)
```

Requirements and text hashes are unique within the certificate. The selected
observation must be a current active SUPPORT observation that realizes the
edge under the certificate's exact policy and currency/validity snapshot.
Certificate rows are serialized by requirement ordinal.

Artifact rows are keyed by `(certificate_digest, requirement_ordinal)`.
Working bindings are keyed by
`(epoch_id, group_version_id, valid_from_revision)`, have at most one open
interval per `(epoch_id, group_version_id)`, and use half-open revision
intervals. The published group-state row points to the selected immutable
digest and records its sealed epoch/revision. Thus repair, rebuild, loss, and
re-completion multiple times inside one semantic epoch preserve exact history
instead of overwriting a `(state_epoch, group)` row.

The certificate is a persisted stateful witness, not a promise that every
implementation independently rediscovers one globally canonical matching:

1. retain the current certificate while all selected rows remain valid;
2. if a selected observation disappears but the same edge multiplicity stays
   positive, replace it with the least active observation ID for that edge;
3. if a selected edge disappears but the group remains complete, rebuild;
4. if completeness becomes true, build; and
5. if completeness becomes false, close the current working binding while
   retaining its immutable artifact and binding history.

An unrelated revision does not duplicate a certificate artifact or binding.
Any decision-policy-version change, even one with zero decision flips, closes
the current binding and, for every active complete group, issues/reuses an
artifact bound to the new policy and opens a new binding. Existing selected
rows may be reused when they all remain valid; otherwise the repair/rebuild
rules apply. This policy-rebind phase may touch every active complete group.

For deterministic reconstruction, each nonzero mask owns an ordered set of
concrete hashes and each edge owns an ordered set of active observation IDs.
Take the least `min(C[m], r_g)` hashes from each mask, order requirements by
ordinal and candidate hashes lexicographically, and run the frozen
augmenting-path constructor. No covering matching needs more than `r_g`
hashes from one mask. This bounds matching work by
`O(r_g^3 2^r_g)` plus ordered representative access, independently of total
witness degree.

The group digest is exactly; rows are serialized in requirement-ordinal order:

```text
stable_m5_digest(
  "m5-group-certificate-v1", *TEXT(decision_policy_version),
  *TEXT(group_version_id), *INT(requirement_count),
  *SEQ(each flattened row encoded as
       SEQ((INT(ordinal), TEXT(requirement_version_id), HASH(text_hash),
            TEXT(selected_observation_id))))
)
```

`certificate_version` is `m5-group-certificate-v1`. Historical certificates
remain interpretable because immutable artifact content, working revision
bindings, and the published sealed pointer are stored.
Incremental/replay paths compare the exact persisted certificate transition.
Independent Python and SQL oracles compare matching size/completeness and
certificate validity, not alternative valid certificate identity.

### M5-D13 -- independent full-recomputation oracles

The independent Python oracle uses exhaustive/backtracking assignment, not the
Hall-mask implementation. At each requirement it explores every unused
neighbor *and* a leave-unmatched branch, then maximizes matched count. Thus
`r1->{}, r2->{a}` has matching size one rather than zero.

The scalable SQL mismatch oracle derives base requirement/hash edges and
enumerates all requirement subsets (`r_g <= 8`), counts each subset's distinct
neighbors directly, and applies Hall deficiency without reading `M[h]`, `C`,
`N`, or any materialized incremental state. A second recursive-assignment SQL
audit uses `UNION` state deduplication over
`(group_version_id, next_requirement_ordinal, used_hash_mask)`, not
`UNION ALL`, and includes an explicit unmatched transition. It runs only when
`H_g <= 16`, `E_g <= 128`, and the preflight upper bound

```text
B(r_g,H_g) = sum over i=0..r_g of
             sum over k=0..min(i,H_g) of binomial(H_g,k)
```

is at most 100,000 distinct states. At the frozen maxima `r=8,H=16`,
`B=90,683`. Any H/E/state cap excess is recorded as
`ASSIGNMENT_AUDIT_CAP_EXCEEDED`, never silently treated as PASS. This bounded
assignment path is a fixture cross-check; the base-edge Hall-subset SQL remains
the scalable third oracle. Neither SQL path imports or invokes either
incremental kernel.

## 7. Claim and answer semantics

### M5-D10 -- direct fast path remains explicit

Existing fields retain their direct-witness meaning:

- `support_count`: distinct direct claim-support text hashes;
- `refute_count`: distinct direct claim-refute text hashes;
- `best_support_score` and `best_refute_score`: direct observations only;
- `supporting_observation_ids` and `refuting_observation_ids`: direct only.

M5 adds:

- `complete_group_count`;
- canonical `complete_group_ids`.

A claim supported only by a group may therefore have
`best_support_score = None`. M5 does not invent a group confidence score.

```text
supported(c) := support_count(c) > 0 or complete_group_count(c) > 0
refuted(c)   := refute_count(c) > 0

supported and refuted -> CONFLICTED
supported             -> SUPPORTED
refuted               -> REFUTED
otherwise             -> UNSUPPORTED
```

Answer aggregation over required claims is unchanged.

### M5-D12 -- versioned claim certificates

M4 direct-certificate digests and sealed rows remain byte-for-byte v1. M5 adds
a v2 tagged support certificate:

```text
support_kind = NONE | DIRECT | GROUP
direct support -> one current direct-support observation
group support  -> one complete group and its matching-certificate digest
refutation     -> one current direct-refute observation, independently
```

If both direct and group support exist, DIRECT is preferred for the compact
claim certificate; complete group IDs and group certificates remain available
for explanation. DIRECT selects the least active direct-support observation
ID. GROUP selects the lexicographically least complete `group_version_id` and
its certificate digest. Refutation independently selects the least active
direct-refute observation ID. Inapplicable fields are absent/NULL, never empty
strings:

| support kind | direct-support ID | group ID | group certificate digest |
|---|---|---|---|
| NONE | NULL | NULL | NULL |
| DIRECT | required | NULL | NULL |
| GROUP | NULL | required | required |

The direct-refute observation ID is independently nullable for every support
kind.

The v2 certificate is likewise an immutable artifact. A working claim binding
uses `(epoch_id, claim_id, valid_from_revision, valid_to_revision?,
certificate_digest)`; the published combined-claim row points to its immutable
digest and sealed epoch/revision. The v2 digest is exactly:

```text
stable_m5_digest(
  "m5-claim-certificate-v2", *TEXT(claim_id),
  *TEXT(decision_policy_version), *ENUM(support_kind),
  *OPTION(TEXT(direct_support_observation_id)),
  *OPTION(TEXT(group_version_id)), *OPTION(HASH(group_certificate_digest)),
  *OPTION(TEXT(direct_refute_observation_id))
)
```

The artifact stores `decision_policy_version` and
`certificate_version = m5-claim-certificate-v2`; its nullable references must
agree with `support_kind`. A change to the selected group certificate dirties
and republishes this claim certificate even when the claim and answer status
enums remain unchanged; it emits no public `StatusDelta` unless an enum
changes. Changes to `complete_group_ids` likewise publish complete claim state
even when count/status are constant.

Unrelated revisions retain the active claim-certificate binding. A policy
change, selected support/refutation change, or selected group-certificate
change closes the working revision interval and binds the new artifact, even
if its status enum is unchanged. Every decision-policy-version change rebinds
every typed-M5 claim certificate, including NONE and direct-only claims, so no
sealed v2 certificate names the previous policy.

Old v1 events, direct certificates, sealed rows, serializers, and logical-job
hashes are never reinterpreted under v2. Existing M4 runtime/rows always use
v1. An epoch created by the typed M5 runtime always uses v2, including NONE or
direct-only states, and never reverts after group retirement. Feature-disabled
execution retains unchanged M4 v1 semantics, identities, serializers, and
result bytes. Shared observation/currency readers receive the claim-only
compatibility filters frozen in Section 10; this mechanical filter is required
for coexistence and is not a v1 semantic reinterpretation.

## 8. Incremental delta algorithm

Maintain:

```text
edge_refcount[(requirement_version_id, text_hash)]
requirement_witness_hashes[requirement_version_id]
group_hash_mask[(group_version_id, text_hash)]
group_mask_histogram[group_version_id]
group_hall_neighbor_counts[group_version_id]
group_matching_size[group_version_id]
group_certificate_artifact[certificate_digest]
working_group_certificate_binding[(epoch_id, group_version_id)]
group_complete[group_version_id]
complete_groups_by_claim[claim_id]
combined_claim_state[claim_id]
combined_answer_state[answer_id]
```

For an event or completion microtransaction:

1. derive signed changes from chunk activation, observation currency,
   requirement/group validity, or policy decisions; if the decision-policy
   version changed, separately schedule a policy-identity rebind for every
   active complete group and every typed-M5 claim, even when no label flips;
2. coalesce changes by `(requirement_version_id, text_hash)` and then by
   `(group_version_id, text_hash)`;
3. update the edge refcount;
4. change the requirement bit in that group's hash mask on every `0 ->
   positive` or `positive -> 0` edge crossing;
5. skip Hall/mask work for multiplicity changes that remain positive, but if
   the removed observation is selected provenance, repair it to the least
   remaining active observation ID for that edge;
6. update each affected group's Hall arrays once for each net hash-mask
   transition and derive exact matching size/completeness;
7. retain, build, rebuild, repair, close, or rebind immutable group-certificate
   artifacts through exact working revision intervals; policy rebind retains
   valid rows but uses a new policy-bound digest;
8. dirty the owning claim on group-completeness/complete-ID changes and when
   its selected group-certificate digest changes, even if status is constant;
9. update a required claim's answer aggregate only when its combined
   `ClaimStatus` changes; and
10. emit public status deltas only for claim/answer enum changes, while sparse
    publication still writes changed full states and certificates.

The earlier rule "propagate to the group only when requirement satisfaction
crosses zero" is incorrect. Counterexample:

```text
r1 -> {a, b}
r2 -> {a, b}
r3 -> {a, b, c}
r4 -> {c, d}
```

This group is complete. Deleting `(r3, c)` leaves every requirement nonempty
and even leaves the global union `{a,b,c,d}` unchanged, but it destroys the
covering matching. Every distinct edge crossing must therefore update the
parent group's Hall state.

Supersession is applied as one coalesced `-old +new` batch. Group replacement
is a before/after structural batch and must not publish a transient state
between retiring the old group and registering the successor.

A completion discovered after its chunk, requirement, group, or containing
epoch becomes inactive is archived with `eligible_for_currency = false`. A
still-RUNNING job transitions to terminal `COMPLETED_INACTIVE` and decrements
open-work accounting exactly once. If the job was already terminal CANCELLED,
the late attempt artifact is appended to attempt-result history with reason
`SUBJECT_INACTIVE`; the job remains CANCELLED and PENDING is not decremented a
second time. Neither case replaces currency, changes an edge, or repairs a
certificate. The same inert archival rule applies to a stale worker from a
failed prior epoch.

## 9. Correctness theorem and proof obligations

### Theorem M5-T1 -- exact state equivalence

Assume:

1. serialized committed semantic microtransactions, including structural
   events, policy changes, and job-completion/revision transitions;
2. valid immutable base records and half-open epoch/revision intervals;
3. one current observation per observation key;
4. deterministic decision policy v1;
5. groups have one to eight requirements; and
6. every semantic microtransaction is failure-atomic.

After initialization and after every committed semantic microtransaction, the
M5 incremental state equals the relational semantics in Sections 4--7 at that
exact epoch/revision snapshot.

Proof obligations:

- signed observation/currency/validity deltas preserve every edge refcount;
- an edge is present iff its refcount is positive;
- every affected group and no logically unaffected group is scheduled after
  an edge or structure change;
- the hash masks, mask histogram, Hall neighbor counts, and deficiency values
  equal their relational definitions;
- the deficiency form of Hall's theorem yields maximum cardinality;
- completeness is equivalent to a covering matching;
- complete-group counts equal the number of active complete groups per claim;
- the total claim and answer truth tables are applied exactly;
- every complete group and every selected claim support/refutation boundary
  has a policy-valid immutable artifact and exact epoch/revision binding;
- a policy-version change rebinds every active complete-group artifact and
  every typed-M5 claim artifact even when it causes zero decision flips;
- rollback restores all pre-transaction base and derived state.

Differential tests against independent Python and SQL recomputation are
required evidence; they do not replace the proof obligations.

### M5-D18 -- bounded affected-group complexity claim

#### Theorem M5-T2 -- affected-group work bound

For one committed microtransaction let:

- `U` be requirement-observation activations, removals, or decision flips
  actually processed after exact policy candidate selection;
- `P` be ordered score-range probes performed to select policy-change
  candidates (`P=0` for a non-policy microtransaction and `P` equals the number
  of changed support/refute threshold dimensions, hence `0 <= P <= 2`, for one
  tie-rule-v1 policy change, even when `U=0`);
- `N_obs` be the maximum active indexed requirement-observation population
  before or after the transaction;
- `Z` be net distinct `(group_version_id, text_hash)` old-mask/new-mask
  transitions after coalescing, excluding net-equal masks;
- `R` be local selected-row provenance repairs that do not require a full
  matching reconstruction;
- `Y` be full matching-certificate reconstructions;
- `G_touched` be group state/certificate keys written;
- `C_touched` and `A_touched` be complete claim and answer keys written,
  including state-only/certificate-only writes without enum changes; and
- `r_max <= 8` be the configured requirement bound.

Expected `O(1)` hash tables provide ownership/refcount lookup. Worst-case
`O(log(N_obs+1))` ordered trees provide policy-score ranges, mask-to-hash
representatives, and edge-to-observation representatives. For stable group
versions, the M5 group overlay performs:

```text
O(P log(N_obs + 1) + U log(N_obs + 1) + Z * 2^r_max
  + R * (r_max + log(N_obs + 1))
  + Y * (r_max * 2^r_max * log(N_obs + 1)
         + r_max^3 * 2^r_max)
  + G_touched + C_touched + A_touched + output_bytes)
```

The implementation maintains ordered hash buckets per mask and ordered active
observation IDs per edge so certificate representatives can be selected or
repaired without scanning total witness degree. For a structural group build,
replacement, or retirement, let `W_g` be active support-observation instances,
`E_g` distinct requirement/hash edges, and `H_g` distinct hashes in the
affected version. Its group-local cost is
`O((W_g + E_g + H_g) log(N_obs + H_g + 1) + r_g 2^r_g)`
plus touched downstream state and output. The logarithmic construction term is
required because structural input is not assumed pre-sorted and the ordered
representative indexes must be built.

Space is linear in active support instances,
distinct requirement/hash edges, distinct group/hash masks, bounded
certificates, claims and answers, plus `sum_g O(2^r_g)` Hall state.

Affected-group augmenting-path recomputation costs `O(r_g E_g)` for that
group. All-group recomputation additionally scans every active group and its
base edges. The Hall-mask kernel removes dependence on `E_g` and `H_g` from a
stable group's Boolean-completeness update at fixed `r_g`; provenance repair
and output costs remain explicit. Dense policy,
corpus, or group-version batches can still be linear in changed inputs and
outputs. In particular, a policy-version change with zero decision flips has
`U=Z=0` but still pays `P log(N_obs+1)` for empty range probes and may enumerate
and write every active complete-group binding and every typed-M5 claim binding;
that work is charged to `G_touched+C_touched+output_bytes`. The bound covers the
M5 group overlay, not the already documented
direct M2/M4 engine. Neural inference, PostgreSQL WAL/I/O, locks, serialization and output
storage are outside the logical RAM bound and must be reported separately;
logical output construction is represented by `output_bytes`. This is a bounded
parameterized result, not unconditional superiority to DBSP, F-IVM, CROWN,
Enzyme, or any named system.

## 10. PostgreSQL physical contract

Migration `014_m5_evidence_groups.sql` must:

1. add versioned group and requirement tables with enforced one-to-eight,
   dense-ordinal, semantic-duplicate, interval, lineage, and immutable-family
   ownership constraints;
2. add and backfill the typed semantic-subject registry plus future-claim,
   future-requirement, and deferred subtype-integrity triggers;
3. replace claim-only observation integrity with a deferrable composite
   subject foreign key; add immutable/backfilled `eligible_for_currency` and
   reject ineligible non-NULL holders on every currency surface;
4. derive current requirement-witness edges from current decisions;
5. add typed M5 working currency revision history, plus
   epoch/revision/policy-versioned requirement and group state, immutable
   group/claim certificate artifacts, working revision bindings, combined
   claim state, and combined answer state surfaces parallel to M4 v1 surfaces;
6. include `complete_group_count` and canonical `complete_group_ids` in every
   M5 working/materialized/published claim projection while leaving M4 v1
   tables and defaults unchanged;
7. inventory every reader and writer in source, tests, and scripts for each
   altered/shared table, require explicit INSERT column lists, and require an
   explicit subject-kind predicate at every claim-only M4 read boundary; static
   checks fail on an unqualified positional INSERT or unfiltered claim-only
   currency read;
8. install the base-edge Hall-subset SQL oracle, capped recursive-assignment
   cross-check, certificate validators, and mismatch views;
9. preserve M1--M4 compatibility surfaces, event IDs, digest versions, and
   feature-disabled logical behavior.

Cross-row rules such as nonempty groups and interval equality require a
deferrable constraint trigger; comments and ordinary CHECK constraints are not
sufficient.

PostgreSQL enforces canonical normalized requirement text rather than trusting
only the application. Migration 014 installs an immutable, strict
`groundloop_normalize_text_v1(text)` function that iterates Unicode code points,
recognizes exactly the 29-code-point set frozen in M5-D1, removes boundary runs,
and emits one U+0020 for each internal run. Requirement rows require both:

```text
requirement_text = groundloop_normalize_text_v1(requirement_text)
requirement_text_hash =
  encode(digest(convert_to(requirement_text,'UTF8'),'sha256'),'hex')
```

Shared Python/SQL golden vectors cover every whitespace code point, adjacent
runs, non-whitespace Unicode, empty-after-normalization input, and the
`"a  b"` versus `"a b"` duplicate adversary. Raw display/source text, if
retained for evaluation, is a separate provenance field and cannot affect
semantic duplicate identity.

There is no pre-existing repository-wide advisory-lock protocol, so migration
014 must not pretend that an ignored advisory key excludes writers. Inside its
single migration transaction it obtains `ACCESS EXCLUSIVE` table locks in this
frozen order:

```text
groundloop_epoch
groundloop_m4_update
groundloop_claim
groundloop_semantic_observation
groundloop_observation_currency
groundloop_published_observation_currency
groundloop_working_observation_delta
```

Only after those locks are held does it reject any row matching the live
single-open predicate `structural_status='committed' AND
semantic_status IN ('pending','complete')`, then perform DDL, backfill, and
validation atomically. Locks release only at transaction end. A concurrent
structural writer test must prove blocking/serialization; prepopulating an
open row or running a shell preflight alone is insufficient.

The physical coexistence route is frozen as follows. M5 shares the already
typed `groundloop_semantic_observation`, current currency, and published
currency relations, strengthened through the typed-subject registry and
subtype checks. M5 requirement admission/job/execution and combined-state
relations are parallel v2 tables. Every M4-v1 bootstrap, reconnect,
reverse-dependency, withdrawal, effective-currency, and repository-loader read
that expects claim subjects must mechanically filter
`subject_kind='claim'`; M4 writers remain claim-only. Existing v1 serializers,
event/job digests, and truth semantics do not change. A populated coexistence
test with active requirement observations must exercise M4 bootstrap,
reconnect, withdrawal, publication, and exact replay before compatibility can
PASS.

`groundloop_semantic_observation` gains
`eligible_for_currency boolean NOT NULL DEFAULT true`; migration backfills all
pre-M5 observations to true, and immutability protection includes this field.
An M5 completion writes false when its chunk, subject/group, or epoch is
inactive/failed at the serialized completion-commit snapshot, immediately
before any currency installation. Currency-write validation on
`groundloop_observation_currency`,
`groundloop_published_observation_currency`, the old M4 working delta, and the
new M5 history rejects every non-NULL holder whose observation is ineligible.
This flag is distinct from task admission: an eligible noncanonical
requirement task may hold its own currency key but remains inert in the witness
view.

The existing one-row-per-key M4 working delta cannot audit two holder changes
inside one epoch. M5 therefore adds:

```text
M5WorkingCurrencyHistory(
  epoch_id, subject_kind, subject_id, chunk_version_id, task_type,
  observation_id?, valid_from_revision, valid_to_revision?
)
```

Intervals are half-open, nonoverlapping, and have at most one open row per
typed key/epoch; NULL `observation_id` is an explicit tombstone. Resolution at
`(epoch_id, revision)` takes the working interval covering that revision, if
one exists, otherwise the holder from the previous published epoch. Each
currency microtransaction closes the prior working interval and opens the new
holder/tombstone at the transaction revision. A history row's typed key,
holder, and `valid_from_revision` are immutable; `valid_to_revision` may change
exactly once from NULL to that closing revision and may never decrease, change
again, or reopen. Rows cannot be deleted, and a failed or sealed epoch rejects
all further history mutation. Closed rows are immutable audit evidence. At seal
only the final holder is promoted into shared epoch-level current/published
currency; all working revision history remains available for certificate
validation.
The M4 final-delta row may be maintained for claim-only compatibility but is
not the M5 as-of oracle.

Group structure follows the existing failure-safe M4 structural-overlay
pattern; pending base rows are not published truth. Every family/group/
requirement row records `creator_epoch_id` and a lifecycle state
`STAGED | PUBLISHED | FAILED`. Immutable semantic content never changes, but
only the coordinator may perform the lifecycle transitions
`STAGED -> PUBLISHED` at seal or `STAGED -> FAILED` at terminal failure.
Replacement/retirement records an immutable epoch-local group-deactivation
overlay with action `REPLACE | RETIRE` rather than closing the published
predecessor early. A sealed RETIRE also appends
`M5GroupFamilyRetirement(group_family_id, retired_epoch_id, event_id)`; its
family key is unique, and deferred checks reject every staged or published
successor of a retired family. Failed retirement overlays create no published
retirement fact.

The published group-validity sidecar carries denormalized, trigger-validated
`group_family_id`, `claim_id`, `semantic_structure_hash`, and predecessor ID in
addition to its epoch bounds. With `btree_gist`, temporal GiST exclusions over
`int8range(valid_from_epoch, valid_to_epoch, '[)')` enforce both nonoverlap per
family and nonoverlap for `(claim_id, semantic_structure_hash)`. A published
predecessor is referenced by at most one published successor. Ordinary partial
uniqueness on group-version rows is insufficient because claim ownership lives
on the family and closed historical rows remain PUBLISHED.

For a working epoch, an effective-group view starts from the previous sealed
published snapshot, removes its deactivation overlay, and unions that epoch's
STAGED successors. Strict reads continue to use only PUBLISHED interval
history. Seal atomically:

1. validates the effective snapshot and exact epoch revision;
2. closes each deactivated PUBLISHED group-validity interval at the sealing
   epoch; requirement activity changes only through its parent group;
3. promotes all current STAGED family/group/requirement rows to PUBLISHED;
4. installs combined state/certificate pointers; and
5. advances the publication head and marks the epoch SEALED.

Failure marks current STAGED rows FAILED and never changes published validity
or strict state. Published-only partial unique/exclusion rules enforce active
semantic duplicates, predecessor/successor uniqueness, and interval overlap;
FAILED rows cannot consume a published predecessor slot. Deferred checks run
against the effective working view before seal. A live test must cover failed
registration/replacement followed by a valid retry, and prove that the old
sealed group remains active after failure.

Migration 014 installs `pgcrypto` for byte-exact digest checks and `btree_gist`
for temporal integrity before creating dependent objects. M5 treats the
ordered pair

```text
migrations/014_m5_evidence_groups.sql
sql/m5/full_recompute_oracle.sql
```

as one immutable `m5-core-schema-bundle-v1`. Its bundle digest is
`stable_m5_digest("m5-core-schema-bundle-v1", *TEXT(path1),
*HASH(file1_sha256), *TEXT(path2), *HASH(file2_sha256))` in that order.

Fresh schema initialization alone is insufficient because the existing
initializer returns early on a populated M3/M4 database. M5 therefore ships an
explicit transactional install/upgrade entrypoint and migration ledger. It
verifies the 013 prerequisite fingerprint, executes both exact bundle files
under the lock protocol above in one transaction, forces deferred constraints
immediate, and records `(bundle_id, bundle_sha256, applied_at)` in that same
transaction. Exact rerun with the same hash is a no-op; the same bundle ID with
a different hash is an error. Fresh installation calls this entrypoint after
000--013; populated upgrade calls it directly. Tests cover both paths,
mid-bundle rollback with no ledger row, and exact rerun. Merely adding 014 or an
unledgered oracle file cannot satisfy M5.3.

M5 has a separate singleton publication head and an explicit activation
barrier. Migration alone does not activate M5. Activation runs once in a
transaction whose base equals the current global M4 publication head,
bootstraps parallel combined requirement/group/claim/answer state and v2 claim
certificates (including zero-group/direct-only/NONE states), and records the
M5 publication head plus active mode. Thereafter every typed M5 seal advances
the global M4 head and M5 head to the same epoch atomically.

Activation, v1 durable open, and typed-M5 durable open serialize through one
`groundloop_runtime_mode(singleton, mode, mode_revision)` row locked
`FOR UPDATE`. A v1 opener holds that row lock from mode check through durable
epoch insertion. Activation holds it while it locks/checks the M4 publication
head and epoch table, rejects any live single-open-predicate row, bootstraps M5,
and flips `v1_only -> m5_active`. A typed opener holds it while checking
`m5_active`, locking both publication heads, verifying head equality, and
inserting its epoch. Consequently activation either precedes a new v1 open and
causes its rejection, or follows its durable insertion and rejects until that
epoch becomes terminal; there is no check-then-open race.

Interleaving new v1 structural events after M5 activation is unsupported and
is rejected before durable epoch open; otherwise the sidecar could silently
miss reverse requirement discovery. Never-activated databases retain the v1
route unchanged, and exact replay/read-only audit of preactivation v1 history
remains allowed. Claim-only compatibility filters are still mandatory because
the M5 dispatcher reuses direct M4 loaders/withdrawal logic over shared
currency and because replay/reconnect audit may run on an activated database.
Preativation replay may return stored results but cannot create/resume v1 work.

### M5-D21 -- typed sidecar authorization and combined-state authority

Migration 014's `groundloop_m4_update_runtime_mode_guard` deliberately rejects
every new M4-v1 mutation declaration after activation. The typed M5 document
route nevertheless has to insert the exact M4-v1 direct declaration inside the
same transaction as its M5-v2 declaration. Migration 015 is therefore
authorized to make one, and only one, semantic change to an object installed
by migration 014: it may use `CREATE OR REPLACE FUNCTION` to replace the body
of `groundloop_m5_guard_v1_open()`. It must not drop, disable, defer, rename, or
replace the trigger, alter migration 014, toggle runtime mode, or weaken any
other v1 guard or relation.

The replacement preserves the `v1_only` branch exactly. In `m5_active` it may
accept a `groundloop_m4_update` INSERT only when rows inserted by the current
SQL transaction establish one matching typed document declaration for the same
epoch. Merely finding a matching row committed by an earlier transaction is
insufficient. The guard validates insertion-transaction identity for the
epoch, M5 update, and typed runtime header, plus all of these bindings:

- the `groundloop_epoch` event ID equals the typed runtime header's structural
  event ID, and that one epoch row remains the shared event/payload binding;
- `groundloop_m5_update.update_kind` is respectively `document_insert`,
  `document_delete`, or `document_replace` for M4 `insert`, `delete`, or
  `replace`;
- the M4 update, M5 update, and runtime header name the same previous
  publication epoch;
- the runtime header and M4 update name the same candidate-policy ID, the
  header binds the stored immutable M5 candidate-policy manifest, and that
  policy's decision-policy version equals both the M4 candidate policy and the
  M5 update's version;
- the M4 update's registry snapshot equals its immutable M4 candidate-policy
  binding; and
- the typed runtime header is the revision-1 `structural_committed`
  declaration for that epoch.

Migration 015 must also install a deferred validation on its runtime header so
a document-kind typed declaration cannot commit without exactly that matching
M4 update, while a non-document typed declaration cannot acquire an M4 update.
Consequently a committed reusable bypass row cannot exist: an activated public
v1 opener has no typed sidecar and is rejected before its transaction can
consume an event ID or epoch, and any mismatched or injected-failure open rolls
back the epoch and both declarations together.

The typed document-open order is runtime-mode and publication-head locks,
`groundloop_epoch`, `groundloop_m5_update`, the revision-1 typed runtime
header, and then `groundloop_m4_update`, followed by both subgraphs, all in one
transaction. No direct subgraph helper may commit, advance a head, or expose
strict state independently.

The shared base epoch's semantic/evaluation state is a combined projection.
A transaction-local M4 helper may compute direct readiness, but before every
typed runtime transaction commits the outer typed coordinator is the final
authority: `complete` is permitted only when the direct M4 coordination
surface is complete and M5 `open_work_count`, `open_scope_count`, and
`blocking_failure_count` are all zero. Otherwise the active epoch remains
`pending`, or becomes `failed` through the typed failure path. In particular,
last-direct-job completion cannot publish a transient direct-only `complete`
state while requirement work remains open.

After activation, public M4 resume, completion, failure, and seal paths must
reject an epoch having a typed runtime header before changing any row. Only
cursor-local helpers invoked under the already-held typed transaction may
mutate its direct subgraph; only the typed coordinator may fail or seal it.

## 11. Dynamic M4 integration contract

### M5-D14 -- typed v2 runtime identity

M5 does not reinterpret M4 v1 pair or job identities. It introduces v2 typed
subjects:

```text
SemanticPairKey(subject_kind, subject_id, chunk_version_id)
owner_claim_id(requirement_version_id) -> claim_id
```

For a claim subject, owner claim equals subject ID. For a requirement subject,
owner claim is reached through requirement -> group -> claim.

Requirement jobs use the frozen requirement text and a separate role-template
identity. Candidate-policy manifests record claim and requirement budgets,
templates, retrieval methods/channels, verifier, prompt/calibration execution
identity, and decision policy; each event payload separately binds its frozen
registry/chunk snapshots. Old `m4-logical-job-v1` hashes and sealed
events remain byte-for-byte stable; M5 uses new v2 job and completion digests.

The immutable M5 candidate-policy manifest contains exactly
`candidate_policy_id`, `embedding_model_artifact_id`, requirement/chunk role
template hashes, vector method version/index kind/build/search configuration
hashes, lexical method/configuration/PostgreSQL/regconfig identities,
fusion version, reverse budget per inserted chunk, forward budget per new
requirement, verifier execution-spec hash, decision-policy version, and the
lineage-safety boolean. Its exact hash is:

```text
stable_m5_digest(
  "m5-candidate-policy-v2", *TEXT(embedding_model_artifact_id),
  *HASH(requirement_role_template_hash), *HASH(chunk_role_template_hash),
  *TEXT(vector_method_version), *ENUM(vector_index_kind),
  *HASH(vector_index_build_config_hash), *HASH(vector_search_config_hash),
  *TEXT(lexical_method_version), *HASH(lexical_config_hash),
  *TEXT(lexical_postgres_version), *TEXT(lexical_regconfig_identity),
  *TEXT(fusion_version), *INT(reverse_budget_per_inserted_chunk),
  *INT(forward_budget_per_requirement), *HASH(verifier_execution_spec_hash),
  *TEXT(decision_policy_version), *BOOL(lineage_safety_override)
)
```

Both budgets are positive. `candidate_policy_id` names this immutable hash but
is not itself hashed into it, matching the existing ID-versus-content pattern.

The v2 runtime identities are byte-exact under the typed expansion in M5-D1:

```text
semantic_pair_digest = stable_m5_digest(
  "m5-semantic-pair-v2", *ENUM(subject_kind), *TEXT(subject_id),
  *TEXT(chunk_version_id))

requirement_registry_snapshot_digest = stable_m5_digest(
  "m5-requirement-registry-snapshot-v2", *INT(requirement_count),
  *SEQ(for each active requirement sorted by requirement_version_id:
       SEQ((TEXT(requirement_version_id), TEXT(group_version_id),
            TEXT(group_family_id), TEXT(owner_claim_id),
            TEXT(normalized_requirement_text), HASH(requirement_text_hash)))))

active_chunk_snapshot_digest = stable_m5_digest(
  "m5-active-chunk-snapshot-v2", *INT(chunk_count),
  *SEQ(for each active chunk sorted by chunk_version_id:
       SEQ((TEXT(chunk_version_id), HASH(text_hash)))))

scope_contract_digest = stable_m5_digest(
  "m5-discovery-scope-contract-v2", *ENUM(direction),
  *OPTION(TEXT(requirement_version_id)),
  *OPTION(TEXT(inserted_chunk_version_id)), *TEXT(candidate_policy_id),
  *HASH(requirement_registry_snapshot_digest),
  *HASH(active_chunk_snapshot_digest))

scope_closure_digest = stable_m5_digest(
  "m5-discovery-scope-closure-v2", *HASH(scope_contract_digest),
  *SEQ(sorted unique selected semantic_pair_digest HASH values))

payload_hash = stable_m5_digest(
  "m5-job-payload-v2", *ENUM(job_kind), *TEXT(candidate_policy_id),
  *HASH(candidate_policy_manifest_hash), *OPTION(TEXT(parent_job_id)),
  *OPTION(HASH(semantic_pair_digest)), *OPTION(HASH(scope_contract_digest)),
  *HASH(requirement_registry_snapshot_digest),
  *HASH(active_chunk_snapshot_digest), *HASH(role_template_hash),
  *HASH(execution_spec_hash), *BOOL(expandable))

logical_job_id = stable_m5_digest(
  "m5-logical-job-v2", *TEXT(structural_event_id), *HASH(payload_hash))

child_set_hash = stable_m5_digest(
  "m5-child-set-v2", *SEQ(sorted unique child logical_job_id TEXT values))

completion_digest = stable_m5_digest(
  "m5-job-completion-v2", *TEXT(logical_job_id), *HASH(payload_hash),
  *HASH(execution_spec_hash), *ENUM(terminal_state),
  *OPTION(TEXT(result_artifact_id)), *OPTION(HASH(result_artifact_hash)),
  *OPTION(HASH(scope_closure_digest)), *OPTION(HASH(child_set_hash)),
  *OPTION(ENUM(archive_reason)))
```

Direction is `forward_requirement` or `reverse_chunk`. Empty scopes still bind
both snapshot digests and use an empty pair sequence. `candidate_policy_id`
names an immutable manifest whose hash is independently validated; changing a
budget, channel, template, verifier, prompt, calibration, or decision policy
changes that manifest hash, while changing a registry/chunk snapshot changes
the scope contract and job payload. Selected pairs are unknown when the root
job is declared and therefore appear only in `scope_closure_digest`, never in
the scope contract or root payload. Golden vectors cover every nullable branch
and child ordering.

The optional scope fields are not freely combinable. Semantic validation
enforces this total direction table:

| Direction | requirement ID | inserted-chunk ID | root job kind |
|---|---|---|---|
| `forward_requirement` | present | NULL | `forward_requirement_retrieval` |
| `reverse_chunk` | NULL | present | `reverse_requirement_discovery` |

Both-present and both-NULL scopes are invalid. Every selected pair and every
child verifier uses `subject_kind=REQUIREMENT`; its `subject_id` is an active
requirement in the bound registry snapshot, its chunk is in the bound chunk
snapshot, and its parent/root and scope direction agree with the table. A
forward closure may select only pairs for its one declared requirement; a
reverse closure may select only pairs for its one declared inserted chunk.

The job kinds and nullable shapes are frozen, not left to that code:

| Job kind | pair | scope | parent | expandable |
|---|---|---|---|---|
| `reverse_requirement_discovery` | NULL | reverse scope | NULL | true |
| `forward_requirement_retrieval` | NULL | forward scope | NULL | true |
| `verify_requirement_pair` | required | enclosing scope | required discovery/retrieval job | false |

Every job binds both snapshot digests, using the canonical empty snapshot where
one side is empty. `COMPLETED_ACTIVE` requires result artifact ID/hash and no
terminal reason. An expandable active completion requires scope-closure and
child-set hashes; an active verifier completion requires both NULL.
`COMPLETED_INACTIVE` requires result artifact ID/hash and reason
`CHUNK_INACTIVE | SUBJECT_INACTIVE | EPOCH_FAILED`; an expandable inactive
completion binds canonical empty scope/child closures, while a verifier binds
both NULL. `CANCELLED` requires no result or closure hashes and reason
`SUBJECT_INACTIVE | SCOPE_RETIRED | EPOCH_FAILED`. `TERMINAL_FAILED` requires
no result/closure hashes and reason `RETRY_EXHAUSTED | RETRIEVAL_ERROR |
VERIFIER_ERROR | INVALID_ARTIFACT`. The `archive_reason` field in the digest
denotes this closed terminal-reason enum; it is NULL only for
`COMPLETED_ACTIVE`.
For every expandable active completion there is an exact bijection between
selected pair digests in the scope closure and declared
`verify_requirement_pair` child jobs in the child set; each child names that
root as parent and the same scope contract, and each child's semantic pair
satisfies the direction-specific restriction above. Closure and children
install in one CAS transaction.

The requirement registry snapshot is the sorted active
`requirement_version_id` set plus immutable
`requirement -> group_version -> group_family -> claim` ownership and each
requirement text/hash. Its identity is a length-prefixed v2 digest. Admission
has two explicit directions:

- an inserted/replaced chunk runs bounded reverse retrieval against that
  frozen active-requirement snapshot; and
- a newly registered/replacement group opens one forward retrieval scope per
  new requirement against the frozen active-chunk snapshot.

The frontier key is
`(requirement_version_id, candidate_policy_id)`. Each scope freezes retrieval
channels, fusion, budget, snapshot identity, and selected pair IDs. Every
declared pair job is required to reach a terminal state before that scope can
close; an empty search may close successfully with zero jobs, leaving the
requirement/group incomplete under policy. Retryable or terminal failures
block sealing exactly as the frozen M4 coordination policy specifies rather
than being converted to semantic REFUTE.

The physical rollout shares typed immutable observations/currency as frozen in
Section 10 but uses parallel M5 admission/job/execution and combined-state
tables. Consolidating M4 and M5 execution tables is outside M5.

Late completion is active only if both the chunk and semantic subject remain
active. Registration atomically creates its new forward scopes. Replacement
atomically retires old ownership/work accounting and creates successor scopes.
Retirement marks every outstanding old scope/job terminal CANCELLED with
reason `SUBJECT_INACTIVE` and decrements each open-work contribution exactly
once; a worker may still append an inert late attempt artifact but cannot
change that terminal job or PENDING. `COMPLETED_INACTIVE` is reserved for a
RUNNING job that was not already cancelled.

GroundLoop CORE retains the single-open-structural-epoch rule: register,
replace, or retire group lifecycle is rejected while the current structural
epoch is semantic PENDING/COMPLETE and unsealed. Therefore an "in-flight
replacement" test means a stale worker attempt from a terminal failed or
cancelled prior epoch returning after a later replacement epoch; it is not two
concurrent structural epochs.

### M5-D15 -- owner-projected PENDING and sealed publication

Open requirement scopes/jobs contribute to their owning claim's PENDING state
whether or not direct or alternative support currently exists; PENDING is
orthogonal to grounding status. They contribute to answer PENDING only when
the owner claim is required by that answer. Optional claims never pend their
answer. Public evaluation objects remain claim and answer; requirement/group
PENDING is explanatory internal state. Strict reads retain the previous sealed
snapshot.

A reverse-chunk discovery root is a lazy scope over every active requirement
in its frozen registry snapshot until its selected child-pair set is closed.
During that interval every owner claim in the snapshot, and only required-owner
answers, is PENDING. Root completion atomically installs the sorted child set,
closes the broad root contribution, and opens child contributions; afterward
only owners of nonterminal child scopes/jobs remain PENDING. A forward scope
has one known requirement owner from creation. Empty scope closure removes its
owner contribution atomically. Direct/alternative support never masks these
open-work counts.

Normal/measured sealing checks transaction-local persisted invariants,
currency/validity, certificates, nonnegative refcounts, scope/job closure, and
coordination CAS surfaces. It performs no inline Python full recomputation or
recursive-SQL audit, and those costs are not hidden inside incremental
latency. Three-oracle equality remains mandatory in audit/test mode and as an
out-of-band check after every measured history seal. A seeded divergence in
that later audit fails the release gate.

## 12. Gold/controlled evaluation contract

### M5-D16 -- primary evaluation provenance

Primary M5 evaluation uses human/source-controlled or explicitly authored
controlled groups. Model-proposed groups are reported separately and cannot
enter the headline group-maintenance result.

### M5-D17 -- WiCE mapping and dual labels

WiCE is the primary large retrospective substrate because it supplies
model-decomposed subclaims with human evidence labels and minimal supporting
sentence sets. GroundLoop already used WiCE in M3 training/evaluation, so M5
must never describe it as an untouched confirmatory benchmark. Mapping is:

The frozen source is the official `ryokamoi/wice` repository at commit
`ddeb6c183665e2a20c5f03c5aa07f03888b9870f`; the manifest records SHA-256 for
each consumed JSONL. WiCE annotations are ODC-BY, underlying text remains
subject to the repository's Wikipedia/Common Crawl terms, and no downloaded
dataset file is committed to GroundLoop.

```text
original compound claim                 -> GroundLoop claim
annotated subclaim                       -> evidence requirement
one annotated supporting-sentence SET   -> one evidence-unit chunk
source human label                       -> source_semantic_label
exact SDR over active evidence units     -> groundloop_sdr_complete
```

An annotated supporting-sentence set is atomic because its sentences may
jointly entail the subclaim; individual sentences are not split into invented
independent witnesses. The cross-product of per-subclaim alternatives was not
itself independently adjudicated, so these use `construction_kind=controlled`
and `construction_source_id=wice:<pinned-revision>`, not a new
`wice_controlled` enum and not unquestioned gold groups.

The official WiCE row does not expose a cited-page identifier. The adapter
therefore defines the provenance-only source identity exactly as
`wice:<split>:<parent_meta_id>:evidence`, after validating that each subclaim's
ID is the parent ID plus one final decimal `-<subclaim_index>` suffix, that the
parent exists in the same official split, and that its evidence array is
byte-equal to the parent row. It never presents this adapter-derived ID as an
original URL or page ID.

Evidence-unit construction is byte-level frozen as
`wice-evidence-unit-v1`:

1. map every annotated sentence index to the derived source identity above and
   preserve `(source_document_id, sentence_index)` provenance in a separate
   immutable member relation;
2. reject a negative/out-of-range/noninteger/duplicate index, missing parent,
   empty set for a SUPPORT label, empty normalized sentence, conflicting text
   at one source index, or malformed membership;
3. normalize sentence text with the exact 29-code-point normalization-v1
   predicate frozen in M5-D1, with no Unicode normalization;
4. sort provenance members by `(source_document_id, sentence_index)` and
   compute `evidence_unit_id` as
   `stable_m5_digest("wice-evidence-unit-v1", *INT(member_count),
   *SEQ(each ordered member as SEQ((TEXT(source_document_id),
   INT(sentence_index), TEXT(normalized_sentence)))))`; `member_count` is the
   canonical decimal integer supplied by `INT`, with no leading zero;
5. independently form the content sentence sequence as sorted unique
   normalized sentence strings. Store chunk text as canonical UTF-8 JSON
   `{"schema":"wice-evidence-unit-text-v1","sentences":[STRING,...]}` with
   sorted object keys, `ensure_ascii=false`, and separators `(',', ':')`;
6. derive `text_hash` by normalization-v1 over exactly those JSON bytes.
   Provenance IDs and indices are deliberately absent, so different units with
   identical textual content collapse to one SDR right-hand hash;
7. render verifier input separately by joining the canonical content sentence
   sequence with two newlines and record its ordinary UTF-8 SHA-256; and
8. never split a set. If rendered input exceeds the frozen
   `fixed-char-v1` limit of 1,200 characters, reject that unit as
   `evidence_unit_overlength`.

The primary retrospective WiCE cohort is frozen to official parent rows whose
parent label is `supported`, that map to 1..8 final released subclaims, whose
every subclaim label is `supported`, and for which every requirement retains at
least one valid nonempty annotated evidence unit after rejection. Parents with
`partially_supported`/`not_supported` subclaims, malformed mappings, or an
unrepresentable positive requirement remain in separately counted audit
cohorts and never silently enter the positive primary denominator.

Canonical duplicate evidence sets coalesce to one evidence unit and one hash;
the adapter reports their multiplicity but cannot use it to inflate edges.
Adapter version, source revision, source-file hashes, encoding hash vectors,
and every rejection reason are recorded.

The mapping is audited, not assumed. One evidence unit can legitimately
support multiple subclaims, while SDR distinctness requires distinct
representatives. The
adapter must therefore report:

- total eligible claims;
- requirements and witness-edge distributions;
- fraction with a perfect matching;
- exclusions and exact reasons;
- results both with and without the distinct-content constraint.

The M5.0 exploratory mirror audit found material SDR-distinctness effects and many
alternative assignments; the checked-in pinned adapter must independently
reproduce all counts before they enter a result. It must retain the
all-requirements-supported but Hall-failing cohort as a primary
SDR-applicability/false-invalidation cohort rather than filter it away or call
it negative gold. Dataset identity, source commit/revision,
file hashes, official split, and license are recorded. No test cluster selects
a policy.

Two labels are never conflated:

- `source_semantic_label` is the human/source annotation projected over active
  units with witness reuse allowed; and
- `groundloop_sdr_complete` is the tested GroundLoop policy derived by exact
  matching.

An all-source-supported Hall failure remains source-supported with
`groundloop_sdr_complete=false`. Exact oracle equality is evaluated against
the latter structured semantics. Semantic utility compares SDR and
non-distinct conjunction predictions against the former. Human/source labels
live in evaluation tables, never in semantic-observation currency. Where
annotations are projected into deterministic engine inputs for a systems
experiment, provenance is a separate immutable relation:

```text
ControlledObservationProjection(
  observation_id, source_annotation_id,
  projection_version="controlled-annotation-projection-v1",
  split_id, manifest_hash
)
```

`observation_id` is unique and references the immutable score row. Reports
must join this relation; they may not infer origin from a model ID, task string,
or score value. These rows are called controlled projections, never model or
gold observations.

The projection bytes are fully frozen. For WiCE, one source annotation ID is:

```text
stable_m5_digest(
  "wice-source-annotation-v1", *TEXT(official_commit), *TEXT(split_id),
  *TEXT(parent_meta_id), *TEXT(subclaim_meta_id),
  *INT(evidence_set_ordinal), *TEXT(evidence_unit_id), *ENUM(source_label))
```

`evidence_set_ordinal` is the zero-based position in the original source
evidence-set outer array before validation, rejection, sorting, or coalescing;
ordinals are never renumbered. Every original annotation, including a duplicate
content set, keeps its own source-annotation row for audit and multiplicity.
When several annotations have the same
`(split_id,parent_meta_id,subclaim_meta_id,evidence_unit_id,source_label)`, only
the least original ordinal is projected into semantic-observation currency;
the remaining rows stay source annotations linked to the same content unit.
Projected representatives are emitted in lexicographic order of that key, then
ordinal, so duplicate input order cannot create an accidental supersession.
Wire values are exactly lowercase: `split_id` is `train | dev | test`,
`source_label` is `supported | partially_supported | not_supported`, and
`projected_label` is `support | neutral`.

Only an unambiguous `supported` source annotation enters the primary engine
projection and maps to scores `(support,refute,neutral)=(1.0,0.0,0.0)`.
An explicitly authored controlled `not_supported` pair, when a negative pair
fixture is required, maps to `(0.0,0.0,1.0)`; it is NEUTRAL, not parent
refutation. `partially_supported` is never projected to a score vector. The
projection uses:

```text
subject_kind     = REQUIREMENT
subject_id       = requirement_version_id
chunk_version_id = evidence_unit_id
task_type        = "verify_requirement_v1"
producer         = ModelStamp(
  model_id="controlled-annotation-projection",
  model_version="controlled-annotation-projection-v1",
  prompt_version="not-applicable-v1")

input_hash = stable_m5_digest(
  "controlled-projection-input-v1", *TEXT(source_annotation_id),
  *TEXT(requirement_version_id), *HASH(requirement_text_hash),
  *TEXT(evidence_unit_id), *HASH(chunk_text_hash), *HASH(manifest_hash),
  *ENUM(projected_label))

observation_id = stable_m5_digest(
  "controlled-observation-v1", *TEXT(source_annotation_id),
  *TEXT(requirement_version_id), *TEXT(evidence_unit_id),
  *TEXT("controlled-annotation-projection-v1"), *HASH(input_hash))
```

`chunk_version_id` equals `evidence_unit_id`. The frozen evaluation decision
policy is `controlled-projection-policy-v1` with support/refute thresholds 0.5
and tie rule v1. Thus one-hot SUPPORT and NEUTRAL are deterministic under the
ordinary observation/currency path. Golden vectors bind source annotation,
score F64 fields, input, observation, event payload, and projection manifest;
the adapter is forbidden to bypass currency by directly writing witness edges.
Every controlled WiCE row enters only through `ObserveRequirementEvent`. No
parent or subclaim label may generate a CLAIM-subject observation. Direct claim
support in a controlled history may come only from a separately identified,
independently annotated whole-claim evidence fixture using the existing direct
claim-observation route; the WiCE primary cohort therefore has direct
`support_count=0` by construction.

The Minimal Evidence Group work is a static conceptual/baseline source, not a
drop-in requirement dataset. SciFact-MEG may be used only where a requirement
mapping exists without inventing gold requirement text. Controlled fixtures
cover alternative groups and adversarial matching cases that public datasets
do not contain. M5 maintains a given bounded group; it does not solve the
set-cover-like minimal-evidence-group discovery problem.

A fresh blinded, independently adjudicated cohort is required before a
real-world semantic-validation claim. Its protocol should use two annotators
plus adjudication over compound claims, explicit requirements, witness
sufficiency, whole-group sufficiency, minimality, alternative groups, and
shared-text adversaries. If no human cohort is available during M5, closure is
explicitly `implementation complete; controlled/retrospective semantic
evidence only` and the human gate remains M6 debt.

### M5-D20 -- semantic confirmation boundary

The absence of a fresh blinded, independently adjudicated cohort narrows the
M5 conclusion; it does not get filled by model output or retrospective labels.
Controlled/retrospective closure is permitted for the implementation milestone
only when the limitation and M6 human-study debt are explicit.

### Dynamic histories

Histories insert, delete, and replace evidence chunks while preserving an
explicit source/controlled witness graph and the two labels above. They
include duplicate content, alternative
witnesses, alternative groups, final-witness loss, matching-only loss, direct
support, refutation conflict, supersession, and policy changes.

### Baselines

Applicable baselines consume identical hash-bound event IDs and the same fixed
stored judgments:

1. **Source-level invalidation.** Freeze the initial page/document versions
   containing the initial citations. Prediction remains supported only while
   every frozen source version is active; the baseline performs no recitation.
2. **Frozen direct-citation invalidation.** At initialization choose the least
   evidence-unit ID per requirement, take their union, and freeze it.
   Prediction remains supported only while every frozen unit is active; an
   alternative unit never silently replaces a lost citation.
3. **Direct-witness-only GroundLoop.** Use independently available human
   whole-claim evidence units only. It is `UNAVAILABLE` for a WiCE compound
   cohort lacking those units; no subclaim witness may be promoted to support
   the whole claim.
4. **Non-distinct requirement conjunction.** Predict support when an active,
   independently annotated whole-claim direct unit exists **or** every
   requirement has at least one active source/controlled witness, allowing one
   text hash to satisfy multiple requirements. The direct disjunct is identical
   to GroundLoop's and is absent on the primary WiCE cohort, so this ablation
   changes only the distinct-representative constraint.
5. **GroundLoop Hall/SDR maintenance.** Use the optimized bounded Hall-mask
   state and exact distinct representatives.
6. **Affected-group full matching.** Recompute exact matching from all active
   edges of each affected group only; same semantics as baseline 5.
7. **All-group full recomputation.** Recompute every active group from base
   edges; same semantics as baseline 5 and the work/correctness comparator.

The applicability and role matrix is frozen:

| Baseline | WiCE primary cohort | Controlled/adjudicated whole-claim cohort | Role |
|---|---|---|---|
| 1 source invalidation | REQUIRED | REQUIRED | different semantic policy |
| 2 frozen citation | REQUIRED | REQUIRED | different semantic policy |
| 3 direct witness | UNAVAILABLE unless independent whole-claim units exist | REQUIRED when such units exist | different semantic policy |
| 4 non-distinct conjunction | REQUIRED | REQUIRED | semantic ablation |
| 5 Hall/SDR incremental | REQUIRED | REQUIRED | target algorithm |
| 6 affected full matching | REQUIRED | REQUIRED | same-semantics systems comparator |
| 7 all-group recomputation | REQUIRED | REQUIRED | same-semantics oracle/work comparator |

Event-hash equality applies among baselines available for that cohort; an
UNAVAILABLE result is reported and never converted to a score. Equal
verifier budgets apply only to strategies that acquire judgments. Pure
invalidation baselines naturally use zero model calls and are evaluated at
that cost; GroundLoop must not manufacture calls merely to make budgets look
equal. A no-dedup ablation, if shown, counts observation rows rather than
distinct unit hashes and is labelled an intentionally incorrect multiplicity
ablation, not baseline 4. Full recomputation is a correctness/work comparator,
not a different semantic label.

### Metrics

For one dynamic claim-history point, define `direct_source_support=1` iff at
least one active independently annotated whole-claim evidence unit supports the
claim, and `requirement_source_conjunction=1` iff every positive annotated
requirement has at least one active source-annotated evidence unit, with reuse
of the same unit across requirements allowed. Source truth is
`y_source = direct_source_support OR requirement_source_conjunction`; on the
primary WiCE cohort the direct term is structurally absent. For each tested
strategy, `y_hat=1` iff that strategy's frozen support predicate holds. For
GroundLoop this is exactly
`support_count > 0 OR complete_group_count > 0`; a CONFLICTED claim therefore
has `y_hat=1` because it is both supported and refuted. Refutation is reported
separately and never silently changes the binary support estimand.

- source-semantic false invalidation
  `sum[y_source=1 and y_hat=0] / sum[y_source=1]`;
- source-semantic false retention
  `sum[y_source=0 and y_hat=1] / sum[y_source=0]`;
- exact structured agreement against `groundloop_sdr_complete` and the three
  oracles, reported separately from source-semantic utility;
- requirement, group, claim, and answer keys touched;
- edge crossings, groups rematched, and matching work;
- verifier pairs/calls/tokens where actual models run;
- update and full-recompute latency;
- state size and certificate size;
- paired confidence intervals from 10,000 percentile-bootstrap resamples of
  the primary claim-history/source cluster with seed `20260802`, plus raw
  event-claim numerators and denominators.

Source/controlled-annotation evaluation and frozen-model diagnostics are reported in
separate tables. V0 may be used diagnostically; V2 remains unpromoted after
M4.13 and cannot become M5's default by implication.

## 13. Atomicity, replay, and compatibility

New event kinds are register group, replace group, retire group, and observe
requirement. They preserve the M1 event contract:

- exact replay is a no-op;
- same ID with different payload is a conflict;
- declaration/domain/transaction rejection before durable epoch open consumes
  neither event ID nor epoch;
- a semantic failure after durable open retains its event ID, epoch, terminal
  FAILED state, staged/attempt audit rows, and exact-replay result, while
  changing no published state; it cannot be described as a rejected event;
- group and requirement structure commits all-or-nothing;
- active completion atomically archives the observation, validates chunk and
  subject activity, supersedes currency, updates edges/matching/full
  claim/answer state, repairs certificates, closes the job, and adjusts
  PENDING; inactive/failed-epoch attempts archive without currency or
  derived-state mutation and cannot transition an already terminal job;
- publication installs working currency and all derived states, emits net
  status deltas, and advances the sealed head in one transaction.

Existing document insert/delete/replace and policy-change event *payloads* keep
their exact existing v1 digest when the typed M5 route uses them. The preserved
M4 structural result types are exactly `OpenEventReceipt` and
`PublicationReceipt`, including the existing
`stable_m4_digest("m4-publication-v1", str(epoch_id))` publication ID. The M1
StatusDelta-tuple result and M4 `EventRunResult` are not claimed byte-identical
under M5 because group-derived deltas and requirement-call counts are new.

A never-activated v1 route retains its original results byte-for-byte. An
activated typed route stores a distinct `M5EventRunResult` containing the two
preserved structural receipts, direct and requirement work counters, combined
claim/answer deltas, and v2 changed-state references. The activation barrier
prevents one new event ID from being processed through both routes. Exact M5
replay returns the stored combined logical deltas/state references with a
REPLAYED marker and zero new calls/writes; it does not append duplicate public
deltas. Preactivation v1 history replay remains untouched.

Every changed-state reference is byte-total. Requirement, group, claim, and
answer state artifact hashes use the four exact `m5-*-state-artifact-v2`
recipes frozen in runtime-addendum Section 10.1; the outer reference binds the
publication epoch and revision. Group- and claim-certificate references use
their immutable certificate digest directly as `state_artifact_hash` rather
than applying a second digest. Activation and later sparse publication use the
same six-kind construction and independently reject a mismatched state hash.

The M5 dispatcher stages the base mutation and M5 overlay in one transaction;
it must not call the old dispatcher to commit first and then rewrite its
receipt. On a never-activated database, feature-disabled code continues to call
the original v1 dispatcher. On an activated database, legacy read/replay audit
and claim-filtered direct components remain usable, but the original v1
dispatcher cannot open/resume a mutation. Importing `groundloop.m5` has no
registration or monkey-patching side effect.

On a schema-upgraded but never-activated database, feature-disabled logical
M1--M4 results and v1 digests remain unchanged through the claim-filtered v1
compatibility route. The typed M5 route is monotonically v2 even when it
currently has zero groups; its direct logical statuses must equal M4 while its
sidecar identities remain v2. Activated databases reject new v1 mutation
opens. Existing direct-only tests and activated read/replay tests are mandatory
regression evidence.

## 14. Frozen M5.0 decisions

| ID | Decision | Resolution |
|---|---|---|
| M5-D1 | Version identity | Immutable owner family and version IDs; adjacent same-family lineage; separate semantic-structure and record-payload hashes |
| M5-D2 | Lifecycle | Whole-group register/replace/retire; one to eight requirements; no in-place edits |
| M5-D3 | Inactive results | Archived but ineligible for currency; prior holder preserved; chunk and subject must both be active |
| M5-D4 | Subject integrity | Composite typed registry/FK plus deferred subtype validation and future-row maintenance |
| M5-D5 | Witness relation | Derived from current active requirement SUPPORT observations; no writable witness link |
| M5-D6 | Completeness | Hall deficiency is zero, equivalently maximum matching covers all active requirements |
| M5-D7 | Incremental trigger | Every net distinct requirement/hash edge crossing changes one group/hash mask |
| M5-D8 | Matching algorithm | Bounded Hall-mask kernel; affected-group augmenting paths remain a baseline |
| M5-D9 | Certificate | One distinct hash and active observation per requirement |
| M5-D10 | Direct fast path | Existing direct fields retain meaning; add complete-group count and IDs |
| M5-D11 | Refutation | Requirement REFUTE means non-satisfaction only; claim refutation stays direct |
| M5-D12 | Claim certificate | Preserve v1; M5 uses tagged/digest-versioned v2 |
| M5-D13 | Oracle independence | Python unmatched-branch backtracking and base-edge SQL Hall/assignment audits cannot read incremental matching state |
| M5-D14 | Runtime identity | Monotone typed subject/job v2 route; old M4 v1 identities and route remain stable |
| M5-D15 | PENDING | Requirement jobs map to owner claim/answer; strict publication remains sealed-only |
| M5-D16 | Primary evaluation | Controlled/human first; model-proposed groups excluded from primary gate |
| M5-D17 | Dataset fit | WiCE is retrospective; byte-stable atomic sentence sets and separate source-semantic/SDR labels |
| M5-D18 | Complexity claim | `O(2^r)` bounded Hall maintenance plus explicit caveats; no general dynamic-matching novelty claim |
| M5-D19 | Requirement task | Only `verify_requirement_v1` observations enter M5 witness state |
| M5-D20 | Semantic confirmation | Without a fresh blinded adjudicated cohort, M5 closes with controlled/retrospective evidence only |
| M5-D21 | Typed direct bridge | Migration 015 may replace only the M4-open guard function so a matching typed sidecar-backed document declaration can insert the exact M4-v1 subgraph; public v1 remains blocked after activation and combined M4/M5 state is outer-coordinator-owned |
| M5-D22 | Changed-state artifact identity | Four byte-total semantic-row digests bind every persisted field; certificate references reuse immutable certificate digests; epoch/revision remain in the outer reference |
| M5-D23 | Runtime transition completeness | Retry errors have a distinct durable hash and exact receipt; cancellation has a byte-total plan; typed direct open/acquire receive the data and cursor-local transaction boundary needed to preserve M4-v1 behavior |
| M5-D24 | Recoverable dispatch and durable accounting | Database-clock leases and total acquisition projections make lost work recoverable; immutable dispatch/execution/work/timing evidence separates confirmed calls from ambiguity; migration 016 and post-terminal sidecars preserve exact replay without changing semantic or M4-v1 identities |
| M5-D24-C1 | Execution disposition and return receipts | Successful execution disposition is explicit; successful requirement/direct receipts separate immutable first-return outcome from current terminal projection and expose exactly one first-write outer timing anchor without changing M4-v1 bytes |

## 15. Release gate

This candidate becomes frozen only when the independent theory, schema/runtime,
and data/evaluation audits agree that:

1. the Hall and matching-without-zero-crossing counterexamples are handled;
2. no lifecycle or referential-integrity ambiguity remains;
3. certificate and v1/v2 digest semantics are total;
4. Python and SQL oracle algorithms are independent and executable;
5. the public data mapping is feasible without inventing gold labels;
6. the acceptance matrix has a falsifying test for every M5-D decision,
   including the M5-D21 typed-bridge exception, M5-D22 state-artifact
   identity, M5-D23 transition completeness, M5-D24 recoverable dispatch and
   durable accounting, and the M5-D24-C1 receipt correction; and
7. path ownership prevents shared-schema or shared-contract collisions.
