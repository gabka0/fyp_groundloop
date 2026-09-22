# M5-D30 Direct-M4 Current-Observation Provenance Amendment

Status: candidate contract only; no implementation, activation, deployment,
performance, or AI-quality authority exists until these exact bytes receive
two independent same-byte `GO`, `P0=0`, `P1=0` reviews and a later
authority-freeze tranche is separately accepted

Date: 2026-09-22

Candidate boundary:

```text
candidate_parent = 8734162f8578ac3105119789a8729fc662da575d
candidate_parent_tree = c821f388ec77fb5415d99bb2f39af378ddb985e2
d29_sha256 = e05f159f98d5f282335a90d2e9db1a26f8d85560030314060d918df59ccc82fb
runtime_addendum_revision = 10
runtime_addendum_sha256 = bbf215fd0d51af41f7e5e3ec7c0558c993dd1150e461283507cb9b0f044d4bff
d29_freeze_handoff_sha256 = 51ed98d60970d9458ae840ed6612baa0f9cf0d4c4dfeee326eb86713adc0d29a
migration_018_sha256 = 941bba975c12e9fb5ba4b4f75a82e59fa518b23eac34468ed2f8b15d1cd9ed90
runtime_mode = v1_only
```

The candidate commit must change exactly this file and have the exact sole
parent above. These identities pin the authority against which the omission
was found; they are not implementation evidence.

## 1. Confirmed contradiction and narrow precedence

M5-D29 makes document withdrawal responsible for every current observation on
a deactivated chunk, but its normative locator and provenance branches are
written only for `subject_kind=requirement`. Claim observations are the direct
M4 path consumed by `StructuralWithdrawal`; omitting them loses a current
holder, while treating them as requirement observations invents M5 requirement
provenance.

D29 also requires a verifier-produced observation to recover its historical
candidate policy through a complete persisted source closure. A broad reading
of that phrase is not implementable from the accepted M4 schema:

- `groundloop_m4_discovery_result` stores a root result, but
  `groundloop_impact_channel_hit` and `groundloop_admitted_pair` store no
  root-membership identity;
- impact and frontier roots may both discover the same semantic pair; and
- event-wide ownership deduplication gives the verifier child to one root
  after discovery, so a root-scope predicate is not its raw hit or admission
  set.

A second broad reading incorrectly narrows D24 to one classic M4 real-model
writer. The accepted typed-direct contract permits an arbitrary nonempty task,
observation ID, result-artifact ID, and producer stamp; its optional
`groundloop_m4_verification_execution` may be absent or may retain a non-NULL
`reused_from_observation_id`. D24 does not universally persist a classic
`PairVerificationArtifact`, pair-input preimage, or pair judgment.

The accepted schema already contains the missing generic link. For a dynamic
M4 verifier completion, immutable `groundloop_working_observation_delta`
records the full observation key, the installed observation ID, and the exact
child-completion revision. M5-D30 uses that row; it adds no surrogate link.

M5-D30 supersedes M5-D29 only as follows:

1. D29 Section 6's requirement-only changed-chunk currency locator is replaced
   by Section 2's one total changed-chunk locator and subject-kind partition.
   D29's requirement branch remains byte-for-byte semantic authority for the
   requirement partition; Sections 2--6 below define only the claim partition.
2. D29's complete-source wording does not require a direct root's raw hit set,
   a root-scope admission set, reason-hit reconstruction, discovery-header
   aggregate reconstruction, classic verifier-artifact derivation, judgment,
   or pair-input preimage for a current claim observation.
3. D29 Section 8's exact-existing permission for the migration-018
   `groundloop_m4_job_by_epoch` range and the dependency primary-key epoch
   prefix also applies once per distinct dynamic claim-observation source
   epoch selected by Section 3. It permits no unrelated epoch or relation scan.
4. At tier 11a, after semantic observations and before current/published
   currency, Section 6 adds exact working-delta point authority. It creates no
   new lock tier.
5. D29 Section 11's held-WIP prohibition is superseded only by Section 8's
   later one-time custody protocol after a D30 authority freeze and a separate
   path-exclusive activation are pushed.
6. A first-application D30 route must already be running at PostgreSQL
   `READ COMMITTED`. The store checks the reported setting before its first
   D30 locator and conflicts for every other value; it does not change
   isolation inside an active transaction.

Every other D24--D29 rule remains authoritative, including exact structural
source identity, same-policy admissibility, predecessor lineage, active
membership, typed-rootless failure, terminal cutoffs, prospective reservation,
counters, timing, replay, D25 transitions, and migration 018.

## 2. Total changed-chunk currency projection

Let `D` be D29's locked, sorted-unique predecessor chunk set. For each chunk in
`D`, exactly one nonlocking range through
`groundloop_current_observations_by_chunk` returns every full
`groundloop_observation_currency` row for that chunk without filtering away a
subject kind. Returned rows are explicitly sorted by:

```text
(subject_kind, subject_id COLLATE "C", chunk_version_id COLLATE "C",
 task_type COLLATE "C", observation_id COLLATE "C")
```

The range is a locator only. Each full currency key, observation ID, and
installed revision is retained for later point authority. Because the enum has
only `claim` and `requirement`, the result is partitioned exactly:

- `requirement` rows follow unchanged D29 Section 6; and
- `claim` rows follow Sections 3--6 of this amendment.

Every returned claim row must match exactly one supported branch:

1. **Dynamic direct-M4 producer.** Section 3's exact working delta and unique
   completed-active verifier child exist, and Sections 4--5 validate their
   sealed direct owner.
2. **Activation-base M3 claim.** Section 2.1 proves that the holder belongs to
   the installed strict activation-base image through exact M3 publication
   closure. Its `current.installed_revision=0` is disjoint from Section 3's
   dynamic installed-revision equality under the frozen M4 writer.

Zero branches, both branches, a third provenance form, or a current ineligible
observation conflicts before D25 DML. Every valid supported claim holder is an
existing structural-withdrawal observation regardless of task. The operational
observation identity remains its exact stored `observation_id`; D30 adds no
edge field or digest.

Claim and requirement rows are never converted into one another. Candidate
edges from M5 requirement admitted-pair history remain D29's separate
projection; neither a candidate edge nor an observation edge is synthesized
from the other.

### 2.1 Activation-base claim branch

This branch extends D29's activation-bootstrap principle to `claim` only. It
does not treat unexplained base bytes as provenance: it validates the exact
bounded M3 publication closure that created the observation. It requires all
of the following:

- the exact installed activation, runtime mode, M4/M5 publication heads, and
  strict activation-base authority validate under D29;
- the current and open-published full claim key name the same eligible
  immutable observation, and that interval covers the activation base and the
  locked predecessor head `P`;
- `current.installed_revision == 0`, exactly matching M3 publication, while
  the published interval is the exact interval selected at the activation
  base; its `valid_from_epoch` need not be zero or equal the observation's
  production epoch;
- the observation has `produced_epoch <= base_m4_epoch_id` and the claim and
  chunk are active in the installed base/predecessor image;
- exactly one immutable M3 `groundloop_verification_execution` row exists by
  observation primary key and cross-binds the published M3 pipeline run,
  claim retrieval candidate, verifier model artifact, verifier prompt
  artifact, calibration fields, raw logits, and raw-output hash under the
  frozen M3 recipes; its `reused_from_observation_id` is NULL exactly as
  written by the frozen M3 publisher;
- that run is exactly `published`, names the observation's semantic epoch,
  and its retained manifest plus exact named artifact-use points agree with
  the execution/candidate/artifact closure; the retrieval candidate belongs
  to that run, is a claim query for the observation's claim/chunk, and names
  its exact embedding artifact;
- the exact immutable `groundloop_epoch(observation.produced_epoch)` point is
  the run's semantic epoch and has
  `event_id == "m3-run:" + run_id`, `payload_hash == run.input_hash`, revision
  `0`, `structural_status=committed`, `semantic_status=sealed`,
  `evaluation_state=complete`, `publication_mode=provisional`, and non-NULL
  `sealed_at`, exactly matching the frozen M3 publisher;
- the M3 observation ID, task, producer stamp, input hash, stored score triple,
  and raw-output hash validate under the frozen M3 identity and publication
  rules. The execution's calibration/logit tuple is retained exactly; D30 does
  not add a score-reconstruction claim for an older accepted row whose M3
  persistence used its frozen no-logits fallback. All named rows are reached
  by primary-key/unique points, never a run, candidate, or artifact relation
  scan;
- no working delta, M4 execution, typed producing runtime/update, direct
  child, job, or admitted-pair absence is inferred or consulted for this
  branch; such unrelated retained history is nonauthority; and
- historical candidate-policy provenance is neither required nor inferred.

The published interval's `valid_from_epoch` and
`observation.produced_epoch` are independent retained activation-base values
and need not be equal; the current installed revision is the exact M3 value
`0`. The new event policy is used only
where the unchanged D29/D25 output contracts already require it. It is not
retroactively attributed to the bootstrap observation.

## 3. Generic dynamic direct-observation link

For each selected dynamic claim holder, define the preliminary source epoch as
the immutable observation's `produced_epoch`. The authoritative proof is:

```text
current and open-published claim currency
  -> immutable eligible claim observation
  -> immutable working observation delta
  -> unique completed-active verify_pair child
  -> exact admitted-pair point
  -> complete owner-epoch job/dependency/scope topology
  -> owning discovery parent and sealed publication
```

### 3.1 Currency, observation, and delta equality

The locked rows must satisfy:

```text
current.full_key == published.full_key == observation.full_key
current.observation_id == published.observation_id == observation.observation_id
current.installed_revision
  == published.valid_from_epoch
  == observation.produced_epoch
  == source_epoch

delta.epoch_id == source_epoch
delta.full_key == observation.full_key
delta.working_observation_id == observation.observation_id
delta.installed_revision == child.completed_revision
```

The current/open-published equality applies only to this dynamic branch. The
task is exact stored text; it is not required to equal `verify` or any other
literal. A typed D24 owner retains D24's nonempty-task requirement. A legacy
owner accepts the exact stored task including empty text because the legacy
schema and writer did not prohibit it. The observation ID and producer stamp
are retained exact bytes, not a derived classic identity.

The complete delta is validated. Its `base_observation_id` must equal the one
observation, if any, effective for the same full key at the owner
`groundloop_m4_update.previous_published_epoch_id`; it must be NULL exactly
when that predecessor had no holder. If that predecessor epoch is NULL, the
store performs no interval query and requires a NULL base. Otherwise the sole
preliminary route is an existing-primary-key backward scan with exact full-key
equality and `valid_from_epoch <= previous_published_epoch_id`, ordered by
`valid_from_epoch DESC LIMIT 1`. The store validates in memory that the one
candidate, if present, covers the predecessor epoch by its `valid_to_epoch`;
a noncovering latest candidate means no effective predecessor and therefore a
NULL base. It retains that candidate's exact primary key, point-locks it at
tier 11a when present, and reruns the same one-row probe under the sealed-owner
and held-head guards. Preliminary, locked, and rerun bytes must agree. The
route visits and returns at most one interval candidate per dynamic holder; it
is not a publication-lineage walk.

Across all selected claim holders, exactly one completed-active
`verify_pair` child in the complete source-epoch job map has
`completed_revision == delta.installed_revision`, and its claim/chunk equal the
delta key. The mapping from selected observation/delta rows to children is
injective. Two task-distinct current observations, two deltas, or any other
rows claiming one child completion revision conflict.

The child has the deterministic frozen job ID and payload, the source epoch,
the exact pair, the historical candidate policy, one parent, one dependency,
the policy's retained verifier execution-spec hash, a successful completed
attempt, exact result ID/hash, completion digest, and state
`completed_active`. The historical candidate policy must equal the locked new
event policy. No cross-policy rebase is permitted.

### 3.2 Exact admitted-pair and parent closure

The deterministic admitted-pair point for the cited child must exist and
agree on source epoch, claim, chunk, and historical policy. Its fused rank,
sorted-unique nonempty reason tuple, and `mandatory_lineage` equivalence are
retained exact admission commitments. D30 does not require a reason to recover
a channel-hit row and does not claim that a retained reason tuple is the
complete raw discovery preimage.

The child has exactly one root parent of kind `impact_discovery` or
`frontier_retrieve` in the same epoch and policy. Parent target, deterministic
ID/payload, exact dependency, successful attempt/completion, child-closed
state, sorted complete child-ID set, `child_set_hash`, and completion digest
validate. Every sibling named by that closure exists in the complete job map
and is in `completed_active` or `completed_inactive`; D30 does not inspect a
sibling's chunk row, observation, currency, execution, judgment, or pair-input
source merely to authorize the cited observation.

The exact parent discovery-result row, when required by the frozen parent
completion, cross-binds the same arbitrary nonempty result-artifact ID and
exact SHA-256 result hash. Its fallback flag, counts, and aggregate hashes are
retained immutable creation-time commitments. D30 does not reconstruct their
unavailable root-local preimages. In particular, it performs no direct-root
hit/admission range and imposes no result-artifact-ID prefix or ID-from-hash
recipe.

### 3.3 Optional verification execution

The execution is optional exactly as in D24. Before tier 8, the store gathers
all three unique coordinates for the selected child and observation:

```text
groundloop_m4_verification_execution(observation_id)
groundloop_m4_verification_execution(job_id UNIQUE)
groundloop_m4_verification_execution(admitted_pair_id UNIQUE)
```

At tier 10 exactly one of these forms must hold:

1. **Present.** All three coordinates resolve to the same immutable execution.
   It agrees with the selected observation, child, admitted pair, execution-
   spec hash, and raw-output hash. Referenced model and prompt artifacts exist
   with their frozen verification tasks. Calibration identity, temperature,
   logits, `pair_input_hash`, and optional `reused_from_observation_id` are
   exact retained fields. A non-NULL reuse value is validated as the accepted
   non-self, FK-backed scalar and is not recursively traversed. Neither the
   observation producer stamp nor any classic artifact/judgment recipe is
   inferred from these fields.
2. **Absent.** All three unique coordinates are absent under the held job and
   admitted-pair guards, and
   `observation.raw_output_hash == child.result_artifact_hash`, matching D24's
   accepted writer-absent insertion rule.

A partial, cross-job, cross-observation, cross-admission, or multiply resolved
shape conflicts. In both forms the result-artifact ID/hash, observation ID,
task, producer, input hash, scores, and any execution fields are retained
exact values. D30 does not reconstruct a `PairVerificationInput`, calibrated
scores, operational label, `PairVerificationArtifact`, semantic-observation
ID, pair judgment, or result-artifact payload.

## 4. Complete owner topology and publication authority

For every distinct dynamic source epoch, the store performs exactly one
migration-018 `groundloop_m4_job_by_epoch` range, one dependency primary-key
exact-epoch prefix, and one discovery-scope primary-key point-or-absence read
per returned job. Explicit C-order sorting precedes every comparison.

The locked/rerun result proves the complete all-seven-state job map, complete
dependency set, one allowed scope shape per job, root/child bijection, no
orphan, grandchild, second parent, or incoming root edge, and globally
sorted-unique verifier `PairKey` values. A successfully sealed selected owner
may contain only `completed_active` or `completed_inactive` jobs. For each
selected delta revision, exactly one job in that total map has the matching
completion revision and it is the cited completed-active verifier child.

The source epoch must lie on the locked predecessor publication lineage under
D29's linear-publication theorem. Failed, open, later-than-`P`, or internally
malformed owner authority conflicts or is excluded exactly as D29 specifies;
it cannot authorize a current holder.

The owner then selects exactly one terminal-evidence branch:

1. **Typed owner.** An exact `groundloop_m5_runtime_epoch` header exists and is
   successfully sealed. Every enumerated terminal job has its exact immutable
   `groundloop_m5_direct_terminal_projection`; the cited child's projection
   agrees with its job state, completion digest, and completed revision. The
   accepted D24 attempt, dispatch/evidence, work, timing, contribution, and
   terminal-result closures validate where applicable.
2. **Legacy owner.** No typed runtime header exists. Exact activation, mode,
   M4/M5 heads, base receipt, and publication authority prove this is accepted
   preactivation M4 history no later than the activation base. Every
   enumerated job has no typed terminal projection. The frozen M4 job,
   attempt, completion, result, working-delta, and publication closure
   validates without inventing D24 rows.

An owner matching zero or both branches conflicts. This classification is
different from Section 2.1's activation-base observation: a legacy dynamic
owner is authorized by a real job and working delta; a bootstrap holder is
authorized without consulting either.

## 5. Retained discovery boundary

For D29 document withdrawal only, the parent discovery row's
`channel_hit_count`, `channel_set_hash`, `admitted_pair_count`, and
`admitted_pair_set_hash` are retained immutable M4/D24 creation-time
commitments. The normal discovery-settlement path remains responsible for
their original validation. Withdrawal neither weakens that creation path nor
pretends to recover missing root-membership bytes later.

Accordingly, these are forbidden as D30 authority:

- a direct-root channel-hit or admitted-pair range;
- an epoch/policy/chunk or epoch/policy/claim scope treated as root membership;
- an epoch-only hit/admission range, `ANY(...)` over either relation, or a
  combined root-kind `OR`;
- unrelated global hits/admissions used to accept or reject the cited claim
  observation; or
- a repository, process cache, retained engine, full oracle, model/provider
  call, or reconstructed classic artifact used as provenance.

The one exact admitted-pair point in Section 3.2 validates the cited child's
own pair and policy; it does not reconstruct a root aggregate. Valid impact/
frontier raw-discovery overlap therefore neither duplicates the observation
nor poisons withdrawal.

## 6. Locator, lock, revalidation, and bounded SQL

The first-application transaction retains D29's tier-1--7 prefix and keeps the
head/event serializers through planning. All D30 locators are nonauthoritative
and complete before tier 8. They gather the selected full currency keys,
preliminary observations, branch classification, working-delta and predecessor
interval coordinates, source epochs, complete job/dependency/scope hints,
cited child/admission/parent/result, attempts, typed projections, optional
M4 execution and referenced model/prompt coordinates, and activation-base M3
epoch/execution/run/candidate/artifact/artifact-use coordinates. The sealed M3
epoch point is gathered and byte-revalidated without an earlier-tier lock
under D29's retained immutable-sealed historical-point rule. No authority key
is first discovered at its lock tier.

Distinct source epochs are deduplicated and sorted numerically. Returned jobs
are deduplicated globally by job ID. Every activation-base M3 coordinate is
deduplicated across holders. Its tier-10 rows are ordered first by relation and
then by primary key exactly as follows:

```text
groundloop_verification_execution(observation_id COLLATE "C")
groundloop_pipeline_run(run_id COLLATE "C")
groundloop_retrieval_candidate(candidate_id COLLATE "C")
groundloop_model_artifact(model_artifact_id COLLATE "C")
groundloop_prompt_artifact(prompt_artifact_id COLLATE "C")
groundloop_pipeline_artifact_use(
  run_id COLLATE "C", artifact_kind COLLATE "C", artifact_id COLLATE "C"
)
```

Shared model/prompt rows are locked once at their existing D29 category, not
reacquired in the M3 suborder. The authority order is:

1. at tier 8, lock/revalidate existing scope points and required absences in
   the accepted order;
2. at tier 9, lock all gathered direct-M4 jobs once in global
   `job_id COLLATE "C"` order, rerun each exact-epoch job range, explicitly
   sort full rows, and require equality with preliminary and locked bytes;
   then nonlocking-reread each known scope point/absence under the job guards;
3. at tier 10, lock complete attempt histories and exact output/result rows,
   then typed terminal projections, dependencies, the cited parent result,
   cited admitted-pair points, the three execution coordinates, and any named
   model/prompt artifacts in D29's existing category order. Activation-base
   M3 branches lock their exact verification execution, published run,
   retrieval candidate, named model/prompt/embedding artifacts, and exact
   named artifact-use points in the matching output/result category. Rerun
   each exact-epoch dependency prefix only after all dependency points are
   held;
4. at tier 11a, lock every selected claim and requirement semantic observation
   in one global typed-key order; next lock/revalidate dynamic working-delta
   points in
   `(epoch_id,subject_kind,subject_id COLLATE "C",chunk_version_id COLLATE
   "C",task_type COLLATE "C")` order; then process current and all named
   predecessor/open-published currency rows in the accepted global typed-key
   order; and
5. finish branch classification and output reconstruction without SQL or an
   earlier-tier acquisition, then continue unchanged D29/D28 tiers 11c--14 and
   D25 finalization.

Dynamic execution absence is protected by the held job/admission coordinates
and their FK/unique routes. Dynamic working-delta presence is rechecked only
after the source owner is proved sealed and while the inherited head/owner
barriers remain held. Bootstrap classification uses no negative M4 provenance
check. This contract relies on the installed GroundLoop writer protocol; it
does not claim safety from an out-of-protocol SQL writer that bypasses it.

Every authoritative row and rerun range must equal its preliminary bytes. A
missing, extra, duplicate, malformed, cross-epoch, cross-policy, changed, or
late-discovered authority coordinate conflicts before D25 DML.

### 6.1 Physical routes and cardinality

M5-D30 authorizes no migration 019, index, table, column, trigger, function,
backfill, DTO, digest, counter, reference kind, or public API. Migrations
001--018 remain byte-identical.

Default-planner `EXPLAIN (ANALYZE, FORMAT JSON)` and executed query traces must
prove:

1. one existing current-currency chunk-index range per changed chunk, returning
   both subject kinds before partition;
2. one migration-018 exact source-epoch job-index range and one dependency
   primary-key exact-epoch prefix per distinct dynamic source epoch;
3. primary-key/unique points for jobs, scopes or scope absence, attempts,
   outputs/results, typed projections, the cited admission, optional execution
   through all three unique coordinates, observations, working deltas,
   current/open-published currency, and model/prompt rows when present; or,
   for activation-base claims, exact M3 epoch, verification-execution,
   published-run, retrieval-candidate, named artifact, and named artifact-use
   points;
4. for each dynamic delta with a non-NULL predecessor epoch, one backward
   primary-key scan on the exact full key and `valid_from_epoch <= predecessor`,
   with `ORDER BY valid_from_epoch DESC LIMIT 1`, followed by an exact point
   lock of the returned candidate when present and the same guarded one-row
   rerun. Plans and traces must show at most one visited and returned interval
   tuple per probe, including for a long same-key history;
5. no direct-root hit/admission range, classic pair-input/citation/source
   reconstruction, sibling-chunk point, judgment lookup, relation scan,
   unrelated-epoch range, or migration-019 dependency; and
6. zero rows removed by filter/recheck wherever the complete predicate can be
   an index condition. D29's bounded bitmap/recheck allowance remains exact.

Let `K` be the number of current currency rows returned across the changed
chunks. For each distinct dynamic source epoch `e`, let `J_e`, `D_e`, `S_e`,
`A_e`, and `O_e` be respectively its returned jobs, dependencies, per-job
scope points, attempt rows, and exact output/result/projection points. Let
`B` be the number of distinct predecessor-publication candidates retained for
selected deltas (`B <=` the number of dynamic claim holders); each preliminary
or guarded rerun probe visits at most one such candidate. Let `M` be the total
exact M3 epoch/execution/run/candidate/artifact/artifact-use points for
activation-base claim holders. The D30 authority width is:

```text
K + B + M + sum_e(J_e + D_e + S_e + A_e + O_e)
```

plus a constant number of exact points per selected claim holder. Attempt
prefix rows are explicitly included in `A_e`; no citation/source-byte term is
hidden because D30 performs no pair-input reconstruction. The trace exposes
every returned row and private sort input. This is evaluation-only query
cardinality, not a work counter, timing identity, or digest input.

Concurrent tests pause each conforming owner writer at the latest reachable
pre-seal cuts and prove publication/seal authority and job/scope reranges
serialize or conflict without a phantom. They separately race current
currency and working-delta classification. The same cases under an isolation
setting other than exact `read committed` are rejected before the first D30
locator.

## 7. Replay and output non-change

Historical replay does not rerun Sections 2--6. It retains D29's exact
event-local source, declaration, patch, contribution, accumulator, result, and
terminal-cut authority and performs no current-head, current-currency,
working-delta, or owner-epoch reconstruction.

For first application, D30 changes only how the existing claim observation IDs
and their historical policy provenance are validated. It changes no successful
supported-history structural-withdrawal DTO, requirement-withdrawal DTO,
fallback key, declaration, semantic edge, work vector, timing identity,
transition, patch, status delta, accumulator, publication row, or result
digest. D24/D25 counters retain their frozen meanings; D30 locator/rerange/
point/sort activity contributes only to query traces and observed database
timing where the existing measurement boundary already includes it.

## 8. Held-work custody and later activation

Acceptance of this candidate would authorize only a later authority-freeze
tranche and a still-later path-exclusive implementation activation. The dirty
`workstream/m5-d29-matching-planner` worktree remains unaccepted evidence.

Before a one-time continuation, every writer process must be stopped. The
activation record must bind the physical worktree, old branch, HEAD/base,
exact pushed authority and activation tips, destination-branch absence, an
index exactly matching HEAD, and the raw
`git status --porcelain=v2 -z --untracked-files=all` bytes and SHA-256. It must
decode every held entry and record path kind, modes where applicable, byte
count, and SHA-256; deletions use explicit absence plus base/index blob
evidence. A complete base-to-activation name/status/mode ledger must prove
path disjointness.

Only then may that same physical worktree execute the one recorded branch
creation at the exact pushed activation tip. Immediately afterward it must
prove exact HEAD/tip equality, an empty staged delta, and identical held-path
kinds, modes, byte counts, and hashes. Any unexplained difference aborts.
Before activation no WIP byte may be committed, copied, cherry-picked,
rebased, stashed, merged, or edited.

The activation must allocate disjoint paths for matching-planner repair,
structural composition, requirement composition, direct-M4 store composition,
direct application composition, public composition, and clean integration.
Every implementation tranche receives fresh whole-byte precommit and
postcommit audits; the earlier passing test snapshot is evidence, not
acceptance.

## 9. Mandatory falsifiers

Implementation or authority evidence is rejected unless every applicable case
passes:

1. One changed chunk containing requirement and claim current holders is
   returned by one total indexed locator and partitioned without omission,
   duplication, conversion, or a second chunk range.
2. Valid dynamic impact-parent and frontier-parent claim observations pass for
   arbitrary exact typed nonempty task, observation ID, result-artifact ID,
   and producer values; a valid legacy dynamic observation also passes with
   its exact empty task. No test depends on an ID prefix or classic digest
   recipe.
3. Valid present executions pass with `reused_from_observation_id` NULL and
   non-NULL. A valid execution-absent D24 observation passes with all three
   execution absences and raw-output/result-hash equality.
4. A missing or changed working delta, wrong full key, working observation,
   base observation, predecessor interval, installed revision, child revision,
   or dynamic currency/epoch equality conflicts. Two observations mapped to
   one child completion revision conflict. A long same-key interval history
   proves that each backward predecessor probe visits and returns at most the
   latest candidate before in-memory coverage validation; no historical prefix
   walk is accepted.
5. A valid activation-base claim holder with `current.installed_revision=0`
   and distinct published/produced epochs passes only with its exact published
   M3 epoch, run, verification execution, retrieval candidate,
   model/prompt/embedding artifacts, named artifact-use points, observation
   identity, calibration, and score/raw-output closure. Missing or changing
   any named M3 row, using a nonpublished/wrong-epoch run, or retaining a
   non-NULL M3 reuse scalar conflicts. Unrelated M4 delta, execution, typed
   producer, job, or admission history is not consulted and neither authorizes
   nor poisons the bootstrap holder; a dynamic row cannot borrow bootstrap
   authority.
6. Independently changing cited child pair, policy, parent, dependency,
   deterministic ID/payload, execution spec, state, completion, attempt,
   result ID/hash, or terminal projection conflicts. Cross-policy history is
   not rebased.
7. Present execution coordinates resolving to different rows, or changing its
   observation/job/admission/execution-spec/raw-output binding, conflicts. A
   changed retained calibration, logits, pair-input hash, model/prompt task, or
   reuse scalar conflicts without invoking a classic reconstruction.
8. The cited admitted-pair point must be exact. An unrelated hit, an admission
   owned by another root, or valid impact/frontier raw overlap neither
   authorizes nor poisons the selected observation. No reason-hit lookup is
   executed.
9. Missing/extra topology, orphan/grandchild/second-parent edges, duplicate
   verifier `PairKey`, incomplete parent child set, wrong root kind/target,
   malformed scope, failed/open owner, or owner later than `P` conflicts.
10. A typed owner with a missing/extra/mismatched projection or D24 evidence
    conflicts. A legacy owner with a typed header/projection or invalid
    activation/head relation conflicts. M3 activation-base, legacy dynamic,
    and typed dynamic branches remain mutually exclusive.
11. A parent discovery result with an independently valid arbitrary ID/hash
    cross-binding passes; changing either retained value conflicts. Changing
    only an unavailable raw aggregate preimage is never simulated or claimed
    as a withdrawal check.
12. Candidate-only, claim-observation-only, requirement-observation-only,
    combined, neither, noncanonical-task, activation-bootstrap, same-policy,
    cross-policy, typed-rootless, and terminal-cut cases retain their exact
    accepted outcomes.
13. Query plans and traces prove every Section-6.1 route and cardinality term,
    exclude other chunks/epochs, and contain no forbidden scan, root-scope
    range, sibling source reconstruction, judgment, or migration-019 object.
14. Conforming publication, scope, delta, and currency races serialize or one
    participant conflicts in both orders, with no phantom acceptance or lock
    inversion. Every non-`READ COMMITTED` route conflicts before planning.
15. No supported successful output byte, digest, work/counter vector, timing
    identity, transition, accumulator, or replay byte changes merely because
    optional execution is absent, task/IDs are noncanonical, reuse is present,
    or unrelated overlapping history exists.

No skipped, xfailed, unavailable, environment-failed, timed-out without
diagnosis, no-match, or silently deselected mandatory case is a pass.

## 10. Non-change and claim ceiling

M5-D30 adds total claim-current enumeration and two supported claim provenance
branches, uses the persisted working delta as the generic dynamic link, and
removes unavailable raw-root/classic-writer reconstruction from withdrawal.
It does not claim support for a claim holder matching neither branch, a
cross-policy dynamic source, or a typed rootless provenance form. Those forms
remain `PENDING` and fail closed.

It changes no creation-time M4/D24 validation, present-state recipe, semantic
or runtime digest, public API/DTO, schema, migration, model/provider/prompt/
calibration choice, counter, timing coordinate, replay identity, reference
kind, publication meaning, or external-call placement.

It makes no deployment, runtime-activation, security, objective-truth,
novelty, maintained-history, performance, utility, AI-quality, or named-system
superiority claim. Runtime remains `v1_only` outside isolated fixtures.
M5-D24 through M5-D30, M5.0-24 through M5.0-30, Task 2, M5.4, M5.5, M5.6,
deployment, and AI/model-quality claims remain implementation-`PENDING`.
