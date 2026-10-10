# Task 17 / D31 Pending-Timing Repair Handoff

Date: 2026-10-10

Status: resumed implementation candidate, NOT accepted, NOT committed or pushed.
Task 17 and C1 remain PENDING. The historical readiness gap below is resolved
by accepted D32 authority/private readiness, not by an authored SQL shortcut.

## Resumption under the accepted D32 sequential activation

The four-path R-T grant is D32_READINESS_IMPLEMENTATION_ACTIVATION.md Section 5,
SHA-256 2e35e3d049d5793ddb0f5ad7af008ac20176ad18d8dca325b84e13e5f53e5740.
Accepted private readiness commit is d716ebb15a07fe703be21d48bf6b037f9253993e,
sole parent 14f8def0f5117ab377f3e91af9c74de07737fb1d, tree
98f8f626d2276cf0f05deef49ab6465df4de53c3. Two identical-four-path-byte candidate
audits and two fresh postcommit checks returned GO/P0=0/P1=0; branch/main atomic
push and live ref verification succeeded. Its final manifest SHA-256 is
c54cc891fefd86455ab3574bfc563927a65125e6b71075a4993c9de2b2a8b135.

Before resumption, root verified every protected/held HEAD, empty index, exact
porcelain-v2 -uall -z hash, all nine C1 and four R-T full content pins. The 25
upstream paths from the historical R-T base were disjoint from all thirteen
held files, and ancestry was proved. A fast-forward-only advance to d716ebb
then preserved all thirteen content pins and raw statuses exactly. No reset,
stash, clean, rebase, cherry-pick or WIP transplant occurred. R-T's current
required sole parent is d716ebb15a07fe703be21d48bf6b037f9253993e; the historical
d7db09c base below is audit history, not the new commit parent.

Detailed implementation prompt / plan:

1. Add only semantic_readiness to the exact nonterminal pending-kind allowlist;
   preserve the same immutable contribution/key/revision and absent timing
   point checks. Keep the reader schema-bound, read-only, transaction-free,
   lock-free, with no timing resolution, history SUM or early constraints.
2. Create local owned REPLACE/RETIRE fixtures using accepted structural/root/
   matching/accounting primitives inside the genuine revision-1 structural-open
   transaction. Do not import held C1 code or bolt on a post-commit D25 image.
   Record actual state-write counts and producer byte regions; open public
   delta count stays zero. Accepted D32 supplies separate genuine readiness
   commits and the coherent final unreported anchor.
3. Test owner and separate login non-owner. The test-only SQL seal adapter
   locks the exact cumulative D24 point, prepares children with the genuine
   pending anchor still intact, adds measured seal work once, inserts the seal
   contribution before terminal runtime, resolves prior timing immutably,
   performs one fused terminal timing CAS and forces intact constraints last.
   Terminal children must equal the unchanged authoritative builder/result.
   This proves reader/SQL-boundary behavior, NOT production C1 terminalization
   or complete physical producer accounting.
4. Assert exact missing timing identity/counts, retained replay with zero DML,
   no duplicate result/contribution/child rows, rollback cuts and terminal/
   half-terminal rejection. Preserve every old deterministic/retained case.
5. Run final owned, D25/store/D29/D30/migration-019 compatibility, pure/static/
   package gates; record failures/skips and do not sum overlapping runs. Freeze
   all four exact files; obtain two same-byte GO/P0=0/P1=0 audits, sole-parent
   commit, two postcommit checks and atomic branch/main push. Only then may
   the still-held nine-path C1 lane advance under its separate gate.

Final-byte audits, commit/postcommit checks and push remain PENDING. C1's nine
files and protected primary/AI stay read-only. Runtime defaults remain v1_only;
no model/API call, deployment, public API/default routing or frozen recipe edit.

## Executed resumed R-T evidence (candidate)

The reader retains all-absent zero extra reads. Coherent pending coordinates
are checked against the unchanged contribution-key recipe and exactly one
immutable work point; an existing timing point rejects preparation. It does
not mutate or resolve timing, lock, start transactions, force constraints or
scan histories. The only change from the held partial reader is adding
semantic_readiness to the twelve-kind nonterminal allowlist. No seventh
changed-state reference kind or present/absence identity change is introduced.

The new REPLACE and RETIRE regressions build their matching foundation inside
the revision-1 structural transaction with accepted helpers, real state-write
counts, measured open byte regions and zero open public deltas. RETIRE uses
the two real D32 commits; REPLACE first uses genuine cancellation, then the
real D32 completion commit. Both yield the immutable unreported
semantic_readiness/semantic_complete anchor at revision 3.

For each action, the owner creates only that accepted matching-open foundation.
The selected owner or separately connected distinct login then performs actual
readiness, pending preparation/seal and retained replay. The actual current_user
is checked. No private-context grant or privilege/validator change is used.
This does NOT establish non-owner structural open: accepted Python matching
preparation reads a definer-owned private temporary transition context, and
that separate boundary remains for C1 to investigate under its own authority.

The local SQL adapter preserves the genuine pending coordinates through child
preparation, locks the cumulative work point, measures publication state-row
writes and exact seal S/K byte regions, and adds that partial seal vector once.
It inserts the immutable seal contribution before runtime terminalization,
resolves the prior missing point without an intermediate timing UPDATE, and
performs one work CAS and one fused terminal timing CAS. Children equal the
unchanged terminal result-bound builder. Intact deferred constraints run last.
SQL traces assert this order, exactly one CAS per accumulator and no history
SUM inside the adapter. A separate exhaustive 32-coordinate contribution SUM
is an independent test oracle only, never the terminalizer's work authority.
Other helper/SQL/validation producer regions are excluded from this explicitly
partial accounting diagnostic. This is reader/SQL-boundary evidence, not
production C1 terminalization, complete physical accounting or utility evidence.

The exact prior immutable missing timing row retains its full key/coordinates
and nine NULL measurements. The frozen coverage is four expected, zero observed,
four missing, no pending coordinates, terminal revision 4. REPLACE has four
changed references; RETIRE has two. Historical replay retains event work and
logical result, has zero call work and leaves the full schema snapshot,
including physical row identities/timestamps, unchanged. For each action all
ten injected cuts (locks, half-terminal, preparation, contribution, missing
point, runtime, work CAS, timing CAS, children, constraints) roll back to the
same complete snapshot before the final legal seal. Terminal preparation
continues to reject. The old deterministic negative matrix is retained.

Environment: owned PostgreSQL 16.14/pgvector container
groundloop-task16-runtime-socket-20261009, host socket
/tmp/groundloop-task16-postgres.1zFwTy. Owned databases are
groundloop_task17_readiness and groundloop_task17_readiness_compat;
PGOPTIONS='-c jit=off'. Executable remains
/home/kassym/Desktop/groundloop/.venv/bin/python, PYTHONPATH=src:.,
PYTHONDONTWRITEBYTECODE=1. Pytest uses -o addopts= -q --import-mode=importlib.
No provider/model call, external data fetch or deployment was performed.

Final candidate-code reports in /tmp/groundloop-d32-sql-diagnostics.v9QrLA:

```text
rt-owned-final-v1.xml: 173 passed, 0 failed, 0 skipped, 70.562 s
  both owned test modules in full
  SHA256 ff830c6e05782d583489e6290ba22d968068a576add6af69a36960c805cbad21
rt-pure-final-v1.xml: 850 passed, 0 failed, 2 skipped, 107.154 s
  tests/m5 --ignore=tests/m5/postgres --ignore=tests/m5/postgres_runtime
  SHA256 d273a13119f897eaaf990b35cf045b78b49b6ca29536026851329ed44d4df1c6
rt-compat-final-v1.xml: 698 passed, 0 failed, 0 skipped, 421.763 s
  d25_store_core, d25_publication, d29_store, d30_store, test_migration_019.py
  SHA256 6a18c861c28ddaf5cb96c2fe1af59f738fd75c3dd2fb96fe82cca0393a8b4514
```

Runs overlap and must not be summed. Pure skips are the unchanged opt-in
100,000-history gate and absent pinned local WiCE source, not new skip/xfail.
The compatibility run includes the complete owned modules again.
Strict mypy of all 148 source files, Ruff lint and format-check of the three
owned Python paths, compileall of src/tests using an external bytecode cache,
and git diff --check pass.

Fresh wheel: /tmp/groundloop-rt-package.1UjssX/wheel/
groundloop-0.1.0-py3-none-any.whl, SHA-256
db340c9a998deb2d629f48c2e170e2e88c8a1a5f60622d000b8bdbbe503cc2c5.
Its 152 entries contain the exact source module and no bytecode, secrets or
root migration assets. An isolated no-dependency install under that temporary
directory passed origin/module checks, twelve pending kinds, six reference
kinds, all-absent zero-read and partial-null fail-before-read checks. Root
migrations remain outside the wheel; installed-wheel migration support is not
claimed. No package configuration or dependency byte changed.

Retained diagnostic failures and corrections (not passing gates):

- rt-genuine-first.xml failed on an incorrect test shorthand for the accepted
  timing accumulator's field names; the test now uses its actual fields.
- rt-genuine-2.xml failed only on the terminal-negative exception expectation:
  the intact 019 attestor raises PostgreSQL RaiseException. The negative now
  explicitly expects that or ValidationError outside its rollback transaction.
- rt-owned-first-expanded.xml: 171 passed, 2 failed, zero skips. Both non-owner
  structural-foundation attempts failed on private transition-context SELECT.
  Owner-only foundation setup corrected the scoped fixture, not privileges.
  SHA256 c311ee73ca662a1823846c6d3714c244798112245efd6afb01d7631d3fc39c75.
  Corrected six genuine cases passed in 29.930 s before trace/oracle additions;
  rt-genuine-foundation-corrected.xml SHA256
  0b27b17af4b45efe32bc8353df8d94ada4f1f9b5927ff2ec83965a1675bf2dcf.
- Initial lint diagnostics for long SQL strings and a loop closure were fixed
  with literal wrapping and an explicit default-bound injected cut name.
- The first no-build-isolation wheel command failed because hatchling was not
  installed in the shared interpreter. Normal isolated wheel building then
  succeeded; only build tooling was fetched. A broad temporary-directory lookup
  met permission-denied system directories and was not treated as evidence.

No failed report was erased or replaced with skip/xfail. Preliminary auditor A
found no new P0/P1 concern and confirmed the fixture boundary; that moving-byte
review is NOT acceptance. The complete four-path freeze, both independent final
audits, sole-parent commit, both postcommit checks and atomic push remain gates.
The external final manifest is
/tmp/groundloop-rt-final-audit.cX0687/rt-final.sha256. Its four complete file
hashes, not preliminary moving bytes, define the candidate; audit and postcommit
receipts stay external so this handoff need not mutate its own frozen bytes.
Protected primary/AI HEAD/index/raw status and all nine held C1 full hashes
were reverified; accepted R32 remains clean. Task 17/C1, remaining M5,
deployment, AI quality and end-to-end performance/cost/utility remain PENDING.

## Historical pre-D32 checkpoint (preserved)

## Authority and custody

The narrow activation was reviewed on exact SHA-256
`9688262fce9340e59cab47e651da9202572e80f09f5b6bfd799e935604e4d94f`.
Independent audits A and B each returned GO, P0=0/P1=0; both repeated their
identity checks after the exact sole-path commit:

```text
activation_commit = d7db09c146795d6e6bef562a0ca266234b397639
activation_parent = 8510470176f0396472d2f125fb8bd010dc3a61c4
activation_tree = 5bd292feb9011507374c40d9286db2c71aef41ec
activation_blob = 2441869168002c28118a3784ab19f93ca5c9f15a
activation_push = atomic branch and origin/main, succeeded
repair_base = d7db09c146795d6e6bef562a0ca266234b397639
repair_branch = workstream/m5-d31-pending-timing-repair
repair_worktree = /home/kassym/Desktop/groundloop-worktrees/m5-d31-pending-timing-repair
repair_index = empty
repair_commit / push = NONE
```

R-T still owns exactly the four paths listed in the activation: the publication
reader, changed-state-reference tests, seal-promotion tests and this handoff.
The seal-promotion test module is unchanged because the new genuine-history
positive cannot yet be constructed under current accepted lifecycle code.
C1's nine held files remain read-only and retain the activation's exact hashes;
no C1 advance or new C1 edit occurred. The protected primary checkout and
AI-study worktree are unchanged. No frozen contract or migration was edited.

## Partial reader correction

The blanket pending-is-NULL check is replaced with a read-only validator:

- all four absent coordinates preserve the old path without extra reads;
- all four present coordinates require a supported nonterminal kind,
  nonempty text source, strict positive revision at or before the expected
  cutoff and canonical contribution-key identity;
- one immutable work-contribution point must match kind/source/key/revision;
- no transition-call timing row may already exist for that complete anchor;
- reads use the already captured schema and point keys, with no new locks,
  transactions, mutation, private-context access or constraint forcing.

All independent half-terminal predicates, D31 accessor behavior, shared
present/certificate/absence recipes, public signatures and terminal builder
remain unchanged. Resolution and the fused terminal CAS remain coordinator
responsibilities; this reader does not perform either.

Deterministic tests cover every supported kind, current/prior defensive
coordinates, all 14 partial-null combinations, terminal/unsupported kinds,
malformed keys, strict type/range checks, contribution absence/drift,
already-reported/conflicting timing, captured-schema qualification and
all-absent zero extra reads. Older-anchor defensive unit cases are not claimed
as genuine complete D24 histories.

## Executed diagnostics, not final acceptance

Executable: `/home/kassym/Desktop/groundloop/.venv/bin/python`;
`PYTHONPATH=src:.`. The live diagnostic used the isolated
`groundloop_task17_repair` database in the owned PostgreSQL 16.14/pgvector
container through `/tmp/groundloop-task16-postgres.1zFwTy`, with connection-local
`PGOPTIONS='-c jit=off'`. No provider/model call or deployment was performed.

```text
pytest -o addopts= -q \
  tests/m5/postgres_runtime/d25_publication/test_changed_state_references.py \
  -k 'not live' --maxfail=1
153 passed, 2 deselected in 0.22s

pytest -o addopts= -q \
  tests/m5/postgres_runtime/d25_publication/test_changed_state_references.py \
  tests/m5/postgres_runtime/d25_publication/test_seal_promotion.py \
  --maxfail=1 --junitxml=/tmp/groundloop-task17-reader-diagnostic.xml
165 passed, 0 failed, 0 skipped in 31.25s

ruff check --no-cache <three authorized source/test paths>: PASS
ruff format --no-cache <two edited Python paths>: one reformatted, one unchanged
mypy src/groundloop --cache-dir /tmp/groundloop-task17-reader-mypy:
  PASS, 147 source files
compileall -q src tests, isolated temporary bytecode cache: PASS
git diff --check: PASS
```

These counts overlap and must not be summed. The 165-test run combines new
deterministic cases with retained live owner/non-owner seal tests, whose timing
accumulators have no pending anchor. It does NOT supply the new live pending
history or justify R-T acceptance. No final candidate byte freeze, whole-byte
implementation audit, package/scale/full-suite gate or postcommit repair check
has occurred. Those gates remain PENDING.

An initial container readiness/database command incorrectly used PostgreSQL's
default internal socket and failed; the configured `/socket` check succeeded
and the isolated database was created before the test. A guessed addendum
filename failed a read; the actual frozen workstream addendum was then read.
Neither failed command is a test pass or schema change.

Final custody check reconfirmed all nine C1 file hashes, all protected HEADs
and raw status digests, and empty indexes. Live origin/main and the activation
branch both equal `d7db09c146795d6e6bef562a0ca266234b397639`. The owned test
container was stopped after the diagnostic; the user's `groundloop-db-1`
remains stopped and untouched. No test database or report was deleted.

## Newly confirmed acceptance reachability gap

Both independent read-only reviewers reached the same conclusion. Confidence:
high for the absence of an accepted group-only readiness route in this base;
this is not a model-quality or public-runtime result.

1. Frozen runtime addendum Section 12 explicitly gives zero-root events the
   empty root-set digest and zero counters, forbids a closure barrier and
   still requires the typed result/seal route.
2. Its Section 14.1 and migration 015 retain
   `structural_committed -> semantic_pending -> semantic_complete -> sealed`.
   Migration 015 requires each runtime transition to increment revision once;
   structural state cannot jump directly to semantic complete.
3. The accepted root builder produces no roots for RETIRE. The root barrier
   rejects an empty root set; cancellation requires nonempty job IDs. There
   is no supported no-work transition/accounting surface for readiness.
4. REPLACE can use genuine acquisition, empty-result staging and root closure
   to obtain a valid latest timing anchor, but those accepted requirement
   helpers always project `semantic_pending`, even when counters become zero.
5. Current `semantic_complete` projectors consume a genuine direct-M4 job.
   D25's direct precursor accepts only document insert/delete/replace kinds,
   not group REPLACE/RETIRE. Inventing a direct job is not a valid repair.
6. The historical `_seal_d26_event` fixture manually authors revisions 1/2/3
   and later installs zero accounting. Carrying a real revision-1 pending
   anchor across those unaccounted bumps violates D24 Sections 7.1 and 9.2,
   even if a synthetic SQL state passes a subset of validators. It cannot be
   relabeled as the activation's genuine pending-history positive.

Concrete read-only pointers:

```text
docs/workstreams/m5_runtime_contract/CANDIDATE_RUNTIME_ADDENDUM.md
  Sections 12 and 14.1
migrations/015_m5_runtime.sql
  groundloop_m5_validate_runtime_epoch_transition
src/groundloop/m5/runtime/persistence.py
  _root_declarations_for_open, _advance_job_lifecycle_revision
src/groundloop/m5/runtime/postgres_roots.py
  close_m5_requirement_roots, _advance_revision
src/groundloop/m5/runtime/contracts.py
  M5CancellationPlan
src/groundloop/m5/runtime/postgres_matching.py
  _direct_precursor, _project_direct_header_after_images
tests/m5/postgres_runtime/test_migration_017.py
  _seal_d26_event
```

Cancellation followed by closure on cancelled roots is not an alternative:
closure requires a staged result, a result-staged scope and a running job.
A fixture that patches readiness headers/projections, manufactures jobs or
skips contribution/timing progression is explicitly rejected. A genuinely
expired-return point on a seeded semantic-complete baseline would only prove
a narrower point regression and would require explicit evidence-boundary
clarification; it is not substituted for the required history gate.

## Proposed next decision and prompt, not new authority

Keep the state graph and add the smallest explicit readiness/accounting
contract for group-only and job-free successful events, including exact
revision/CAS, one outer-transaction timing anchor, replay and durable work
identity. This requires a new frozen decision and path activation; it cannot
be implemented by editing the four R-T paths or silently borrowing C1 paths.
Do not select or invent a new contribution recipe in this handoff.

Suggested contract-first prompt:

> Audit the confirmed group-only readiness gap against D24--D31. Draft the
> narrowest amendment that preserves the epoch graph, zero-root no-barrier
> rule, exact contribution/timing accounting, replay, independent readiness
> checks and terminal publication order. Name exact affected validators,
> recipes and source paths. Obtain two independent same-byte P0=0/P1=0
> contract reviews before authority freeze and a sequential path-exclusive
> implementation activation. Preserve the held C1 and reader WIP. Then prove
> genuine zero-root RETIRE and nonempty-root REPLACE histories, finish this
> reader prerequisite and resume C1. Keep public routing, deployment and
> AI-quality claims PENDING.

Until that decision is authorized and frozen, preserve this partial repair
without committing or pushing it. Runtime defaults remain `v1_only`; Task 17,
Task 2, M5.4--M5.6 and all broader claims remain PENDING.
