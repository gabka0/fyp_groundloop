# S36 Narrow Packaged-SQL Prerequisite Ownership Amendment

Date: 2026-10-20 (Asia/Hong_Kong).

Status: docs-only CANDIDATE; no source authority before Section6 acceptance.

Operational ownership correction, not a semantic decision or gate waiver.
The user's standing approval to continue all normal in-scope prerequisites
covers this draft, complete independent audits, acceptance/publication and the
sequential implementation below. No repeated preparation-only approval is
required. Each acceptance gate still requires actual evidence; no future hash
or passing result is presumed. Root is sole editor; two auditors own ZERO paths.

## 1. Literal parent and confirmed installed-artifact failure

```text
parent = 10eab43bdfc46ea64d813ba98b304a7e199acbac
parent_tree = 3a2748f0c4b37cbb86b15e2a434ddaa9f4f8ad35
tooling_amendment_sha256 = 42152c552bcbee74ade1c52bffcc9b54a7d447cb1caafdc1024eea162a103231
tooling_publication_receipt_sha256 = e8f1fa11623df7fa13403a324915da9b9e0ac4cb874b328fc7a0ed72dec81eae
S36_advance_requalification_receipt_sha256 = 19d891e3ea651f588365ce6c919b3b3a2ac0ddbe9a41c182bde6e297cce2f718
tooling_root = /home/kassym/Desktop/groundloop-s36-build-tooling.7RXWZ9rR
build_closure_manifest_sha256 = 4266b998c915f9090093348133a0bbce7e510a5ae4a4929c735962338885744c
build_lock_sha256 = d43f60dc39a2536d2e568cf0de2e56ba9bb1cdb88b174e6aaef9bab3939ca1d8
real_wheel_diagnostic_sha256 = 5fad89a3d90d01c214b7bfd5d6c4372e4b0efaabe08f944986ae1690859823e5
wheel_sha256 = e64f28fb5d7f9402ce1b097d859b60a4e634f9e03db7ec8220f3e4a7b193978a
archive_inspection_sha256 = f756b8cf7b9cb8f95c71deab684370231354915e05a570c250730c1fc5a89e3f
installed_probe_sha256 = c8086247e3b45f7b4bfd7922304714e87d789f4515e200436ca79dd5a97869db
checkout_identity_baseline_sha256 = 8c31a90dec68da1883675d42896d0db28c3c62ee74bc7c6c3ce46246505f4b5c
```

Both independent complete build-closure audits returned GO/P0=0/P1=0/P2=0
before offline hash-locked installation. Only the declared Hatchling backend
and its five active build-only dependencies were installed in the disposable
venv. Pip check, actual wheel build and clean target installation passed.
The original missing-Hatchling build failure remains retained separately.

The real wheel contains ZERO SQL members. Its RECORD is valid; all20 loaded
GroundLoop modules came from the clean installed target, not the checkout.
The real installed ROOT resolves outside that target. All24 required resource
checks and all9 existing administrative identity calls failed; the same9
identity calls succeed from the checkout. This is a confirmed packaging and
locator defect, not an AI-quality, database, dependency or SQL-behavior result.
The offline diagnostic completed with explicit HOLD; no SQL/DB/provider call
or source edit occurred. Full protected custody before/after is unchanged.

## 2. Exact exceptions and deliberately separate source tranche

S36 activation Sections3/8 and tooling-amendment Sections3/7 yield ONLY for the
following FOUR source paths in a NEW sequential packaging worktree:

1. `pyproject.toml`: append ONLY a wheel-target force-include table mapping
   `migrations` to `groundloop/_sql_resources/migrations`, and
   `sql/m5/full_recompute_oracle.sql` to
   `groundloop/_sql_resources/sql/m5/full_recompute_oracle.sql`.
   Preserve every existing byte, dependency/build requirement, version, extra,
   entry point, package selection, formatter/test/type setting and other table.
2. `src/groundloop/postgres/migrations.py`: ONLY add the private helper import
   `from groundloop.postgres._resource_root import _sql_resource_root` and
   replace the ONE binding `ROOT = Path(__file__).resolve().parents[3]` with
   `ROOT = _sql_resource_root(Path(__file__))`. No other byte may change.
   In particular NO existing function body, dispatcher, digest, label, path
   constant, accepted literal, signature, catalog inventory or checker changes.
3. `src/groundloop/postgres/_resource_root.py`: NEW private standard-library-only
   locator. Bind resources to the imported module's package, never cwd,
   PYTHONPATH, environment, caller-supplied resource roots or ancestor search.
   Installed wheels use ONLY their own `groundloop/_sql_resources` tree.
   Missing/partial packaged files must fail, not fall back to a checkout or an
   adjacent migrations directory. Preserve supported Git source checkouts via
   their exact module-relative `src/groundloop` layout and local Git/pyproject
   markers; packaged resources take precedence even in that source-shaped
   layout. Do not broaden this source-layout exception into discovery or repair.
4. `tests/m5/runtime/test_packaged_sql_resources.py`: NEW pure tests for the
   exact ownership transforms, unchanged checkout location/identity vectors,
   package configuration, source-versus-installed layout, packaged precedence,
   missing/partial resources and rejection of cwd/adjacent-tree fallback.

No directory-wide grant. No public export, dependency declaration, build hook,
version bump, old SQL/oracle edit, copied tracked SQL duplicate, model/provider,
runtime producer, schema/ACL/default-mode or matching change. Hatchling's normal
wheel inclusion is authorized; manually copying SQL into a wheel/positive test
installation is NOT. Snapshot/application/CLI consumers outside these four
paths remain unowned; this is the administrative-resource prerequisite only,
not evidence that every installed public workflow works.

## 3. Worktrees, ownership and unchanged protected checkpoints

This docs worktree is
`/home/kassym/Desktop/groundloop-worktrees/m5-d36-s36-packaged-sql-amendment`,
branch `workstream/m5-d36-s36-packaged-sql-amendment`, sole parent Section1.
Root owns ONLY this NEW document during drafting. External evidence is
`/home/kassym/Desktop/groundloop-s36-packaged-sql.V6sgpDuW/`.
PRECREATION.json SHA256
502ff5d8f7a8e6b9732e721622c8cc0ab0975df8d4170abd6983701a0fdd6769
pins ALL151 preexisting worktrees,774 parent blobs, held S36/P33/C1 states and
all15 container identities/names/states before this152nd worktree.

After accepted docs publication, record its literal commit/tree and create
NEW `m5-d36-s36-packaged-sql` worktree/`workstream/m5-d36-s36-packaged-sql`
branch from THAT accepted literal commit. Do not use a moving main reference.
Capture a fresh precreation inventory of every old checkout and full protected
pin before creation. Root alone owns the FOUR Section2 paths there; no source
editor works in S36 simultaneously. Existing source files start from accepted
parent bytes, not held-WIP extraction/transplant. Record old/new exact global
binding/import deltas separately from S36's two eventual dispatcher bodies.

At the current source parent, pyproject SHA256 is
f5dc5e99a74731eafa97a365e8e3c40f40c08dd960a7b7d520a9582c2c459010,
blob745beb02febb073fca52d16ab5ee45136602c79d; migrations.py SHA256 is
368e2343d57a9c07570d698b1e247c24c456ab7f601a10a1c549738685939716,
blobb11db63f1ba3b4ee461dca141f4aba55911b8acd. New helper/test are absent.
S36 remains at10eab43b with EMPTY index, unchanged migrations.py and ONLY the
held untracked handoff SHA256
287297f70528aff2a678e67f35a1ac565d8ef7e118d83c617e11cf93305b21ac.
Original HELD_CHECKPOINT.json SHA256
03e6776869ef96c993bc9b23dee1e14826e613cc898b0ac651ee0314554f0779
and all failure/baseline/advance receipts remain immutable.

P33 remains at67bfe7ffafd41f71ebfd34b82710698773367778 and C1 at
8290d47cba71350cc550cf18cb12c32b47cb7b2a, original full four/nine states and
empty indexes exact. No advance/edit/reset/stash/clean/rebase/cherry-pick,
copy/extraction or overlap reconciliation of those checkpoints is authorized.

## 4. Reuse audited disposable builder, then real artifact qualification

Use ONLY the existing Section1 disposable root/environment/verified wheelhouse.
No re-resolution, network, additional dependency installation, shared/project
environment edit, model fetch, DB/container/role mutation or SQL execution.
Revalidate the exact six-wheel hashes, lock, installed build versions/origins
and pip check. Preserve the original frozen pyproject selection-input hash;
record the new build-input hash separately. Its build requirement stays exact,
so this is not a retroactive manifest rewrite or new dependency selection.

Create NEW owned output and installed targets under that tooling root. Retain
all failed baseline artifacts and reports. Build with --no-deps,
--no-build-isolation and --no-index; install the actual wheel --no-deps and
--no-index, without copying resources afterward. Validate safe archive paths,
RECORD and exact SQL bytes against unchanged000--022 plus M5 oracle sources.
Normal directory force-inclusion must also package023 once it legitimately
exists later; it neither creates023 now nor authorizes any023 execution.

Outside ALL checkouts, use -I -S -B with clean target FIRST and read-only
existing project-venv dependencies AFTER, without running .pth/customization/
editable finders. Require every GroundLoop module origin inside that target.
Exercise the REAL legacy path list and all9 no-override bundle identity calls:
all24 payloads must exist INSIDE the installed package, match source bytes,
and produce the exact Section1 checkout-baseline identity vectors. Do not
monkeypatch ROOT, resource helpers or identity inputs. Record dependency origins
and actual commands/times/exit statuses; no missing dependency auto-install.

Also install the same real wheel into a SECOND new negative-test target under
the owned root. Reversibly rename its packaged resource directory to a named
owned quarantine child, then show real lookup/identity calls fail even with
the checkout as cwd and available adjacent/source SQL. Do not import source
GroundLoop or fabricate resource contents; the negative must remain a failure.
Retain the wheel, target/quarantine identities and exact negative logs.

Run complete new pure tests and complete existing tests/m5/runtime, with no
deselection/xfail/skip workaround. Run relevant affected administrative pure
tests, Ruff/format/type/compile/diff checks and whole-repository offline
diagnostics, recording counts separately and retaining known unrelated failures
and optional skips. An unrelated diagnostic failure is not permission to edit
its paths or claim all-green. Mandatory owned tests and the real positive/
negative artifact boundaries must pass as specified. Record all old SQL,
all old existing-function bytes and every identity vector unchanged.

Freeze FOUR COMPLETE source paths and evidence; obtain two complete independent
same-byte GOs/P0=0/P1=0, exact sole-parent source commit, two FRESH postcommit
checks, atomic no-force main/source-branch publication and exact live refs.
Only then is this scoped packaging prerequisite accepted. No S36/D36 PASS.

## 5. Explicit checked same-S36 advance and H36 ancestry correction

ONLY after both docs and FOUR-path source publication, record their actual
literal commits/trees and publication receipts. This section is the NEW narrow
advance exception; the older wholly disjoint-only rule alone is insufficient.

Before advancing SAME held S36, prove its HEAD is STILL10eab43b, index EMPTY,
raw dirty status/handoff bytes exact and NO local migrations.py/pyproject diff.
Verify all original twelve path states/full bytes/modes/blobs, original two
dispatcher-body/complement pins and all P33/C1/protected worktrees/containers.
Check ENTIRE incoming ancestry is exactly TWO sole-parent commits: this one
NEW doc followed by the accepted FOUR-path packaging source, with no merge or
intervening commit. Its endpoint AND every-commit touched-path union must equal
ONLY this document and the FOUR Section2 paths, disjoint from all held P33/C1
paths. The sole overlap with original twelve S36 paths is migrations.py; its
incoming delta must be EXACTLY Section2's new import and ROOT binding, with
both future editable dispatcher bodies and EVERY other old byte unchanged.
No other overlap, dirty tracked file or unknown ancestor is allowed.

Then ff-only advance SAME S36 to the literal accepted packaging SOURCE commit,
autostash disabled. No reset/recreation/stash/rebase/cherry-pick/source copying.
Prove all preexisting index entries unchanged EXCEPT exact accepted pyproject/
migrations blobs and exactly THREE new tracked entries (this doc, the new
helper and new test), independently enumerated against BOTH accepted trees.
Require the entire new index equal the accepted source tree, EMPTY
staged diff and unchanged raw untracked handoff. Every old twelve-path state
remains byte-identical except the explicitly authorized migrations globals.
Record exact added/changed index entries, full old/new trees and all checks in
a NEW advance receipt; do not modify old failed/hold/baseline receipts. Create
a NEW read-only custody verifier for this accepted parent, proving the original
complement by reversing ONLY the two authorized global edits before checking
the original pins. Packaging globals/helper/config/tests then become READ-ONLY
upstream bytes; S36 retains ONLY its original twelve paths/two body grants.

For H36 ONLY, replace the tooling amendment's Section4 parent correction with
the literal accepted packaging SOURCE commit just recorded, which must still
be published main. Preserve original H36 worktree/branch/path, twelve-file
S36 draft/audit barrier, separately pinned SQL/catalog/bundle/role artifacts,
checked H36 advance and final S36 source sole parent=accepted H36. No023 SQL
executes before H36, including raw diagnostics. Unknown literal pins stay later
gates, never guessed or learned from a live target. If main/ancestry drifts,
stop for separate audited ancestry authority, never refresh old baselines.

## 6. Acceptance and claim ceiling

Freeze this COMPLETE one-path candidate's SHA/blob/bytes/lines/mode/parent/tree
externally; obtain two independent complete identical-byte GOs/P0=0/P1=0.
Every edit revokes BOTH. Then exact sole-parent commit, two FRESH postcommit
checks, atomic no-force main/docs-branch publication and exact live refs activate
only these bounded future steps under the standing user approval.

No D36 semantic correction, runtime-addendum change, tombstone, reference kind,
nullable hash, repr/JSON hashing or present-state recipe is introduced. All
eleven D36 groups and original P33/D26/D33/C1 gates remain mandatory/PENDING.
D25 integration, M5.4--M5.6, Task2/Task17, deployment/security, AI quality and
end-to-end utility/speed/cost remain PENDING; runtime stays v1_only outside
isolated fixtures. Paid calls/spend/reserved remain0 under aggregate US$5.
Task14 labels remain self-reported AI-assisted project-author review, not
independent blinded human adjudication. The four-step campaign is unfinished.
