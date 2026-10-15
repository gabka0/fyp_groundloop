# D34 C2 narrow status-delta ownership amendment

Date: 2026-10-15. Candidate; source authority begins only after the complete
two-audit/commit/postcommit/push/live-ref barrier below. User approved this
narrow amendment after the independently confirmed event-ID blocker.

## 1. Exact parent and docs-only scope

```text
sole_parent = c69867e8a7165f150907dbcb9f32e25be75c6ee9
parent_tree = 2fc0e711338d241507ecae851084e8a06b0cc876
original_activation_sha256 = 0c07d7e0fa74d223a60e5f03f67aa1cc6f137ebc3f0d0a4585623f39ddffb179
branch = workstream/m5-d34-c2-status-delta-ownership
worktree = /home/kassym/Desktop/groundloop-worktrees/m5-d34-c2-status-delta-ownership
sole_docs_path = docs/workstreams/m5_runtime_implementation/D34_C2_STATUS_DELTA_OWNERSHIP_AMENDMENT.md
```

Root owns this one NEW document; two independent auditors own zero paths.
Source/tests/migrations, held source worktree, DBs/containers/providers and all
other authority remain read-only during this docs tranche. Original accepted
D34 amendment and thirteen-path C2 activation bytes remain immutable.

## 2. Reproduced implementation defect

Accepted `_requirement_logical_transition` puts `source.attempt_id` into both
claim/answer `StatusDelta.event_id` fields. Migration001 requires that field
to reference `groundloop_epoch(event_id)`; migration017 requires the exact
six-field encoded logical-output multiset; D28 requires all its tier16 rows.
The live native-plan regression produced two nonempty status deltas whose
attempt IDs matched NO epoch event. Counter issue is separate and reconciled:
D25 planned transition-history cardinality is not a non-seal D24 charge.

No lawful workaround remaps only INSERT, rewrites prepared output, invents an
epoch or skips nonzero status changes. Both independent source reviewers
confirmed this existing implementation defect. No frozen contract prescribes
attempt ID as the event field; this is ownership repair, not a new semantic,
schema, public API, digest recipe or counter-owner amendment.

## 3. Exact extension — same thirteen source paths

Only within already-owned `src/groundloop/m5/runtime/postgres_matching.py`,
extend the original existing-symbol grant by:

1. Private `_RequirementCompletionSource`: add exactly the checked
   `structural_event_id: str` field; preserve every other field and meaning.
2. `_requirement_logical_transition`: change ONLY the two claim/answer delta
   event-ID selections from attempt ID to that structural event field.

The already-owned `_requirement_completion_source` may extend its held
authority projection, validate `epoch.event_id == runtime.structural_event_id`,
and populate the field. Its single private constructor must use keyword
binding. No new earlier lock, global lookup/cache, implicit attachment or
caller-authored event authority is introduced. Existing native intent/patch
production must still independently recompute the corrected first application.

Keep the real attempt/source IDs and attempt-bearing reason unchanged. Keep
all public signatures/DTOs/exports, field orders/encodings/domains, matching
semantics, counter ownership, migration000--021 bytes/ledgers and runtime
defaults unchanged. Existing retained patch/result/contribution bytes remain
immutable; replay validates their original stored projection, never repairs,
remaps or recomputes historical intents with the new first-application value.

No other function/class/body or source/test path is added to ownership. All
original narrow grants, complete falsifiers, independent full-image/history,
qualification, acceptance and downstream barriers continue unchanged.
In particular private completion is not public C1/store/application wiring.

## 4. Literal custody and checked source-parent advance

The EXISTING source worktree is
`/home/kassym/Desktop/groundloop-worktrees/m5-d34-requirement-completion`,
branch `workstream/m5-d34-requirement-completion`, HEAD/sole current parent
`c69867e8a7165f150907dbcb9f32e25be75c6ee9`, tree
`2fc0e711338d241507ecae851084e8a06b0cc876`, empty index, four tracked
modifications/four untracked files. Full eight-file checkpoint is
`/tmp/groundloop-d34-c2-source.20lCffBL/HELD_CHECKPOINT.json`, SHA256
`52911fb682f00c71105fee2b241ce018840b2f5a81c89c4356627f79a70c6899`.
Raw-v2-z and every full path/byte/hash/mode/blob pin in that manifest must
match before and after any permitted advance. Recheck receipt availability.

Only after THIS sole-file amendment has two independent identical-byte
GO/P0=0/P1=0 audits, exact sole-parent commit, two fresh postcommit identity/
custody checks, atomic no-force branch/main push and verified live refs may
root record its literal accepted commit/tree from the postcommit receipt.
No unknowable future hash is supplied here.

That literal accepted amendment commit supersedes ONLY the original
activation's source-sole-parent choice for this checked advance. Verify the
entire incoming ancestry from c69867e: exactly ONE linear sole-parent commit,
only this NEW docs path, no merge, endpoint diff AND touched-path union
disjoint from every eight held source paths, five B33 paths and nine C1 paths.
Recheck complete source bytes/modes/blobs, HEAD/branch/index/raw inventory and
protected custody, then perform ONLY
`git -c merge.autostash=false merge --ff-only <literal_accepted_amendment>`
in the EXISTING source worktree. Recheck every pin afterward; raw inventory
must remain identical. No stash/reset/clean/rebase/cherry-pick, WIP transplant
or new source checkout is authorized. Every failed prerequisite stops.

After successful checked advance, the eventual COMPLETE source candidate's
sole parent is that literal accepted amendment commit. This changes neither
the original thirteen source paths nor final source acceptance barriers.
Do not commit the partial eight-file WIP as accepted C2.

B33 remains at4f50dc6701b462024e125a45505eb7be17bd4112 with full five-file
manifest SHA2560699d07c659a80802e623af59ab55bbdf62c1d89aa8f36fb977526751b992552.
Current nine C1 pins, old C2, five protected inventories and nine accepted
prerequisites remain exact; use immutable D34 handoff/current D33 C1 pin table.
This is not the future B33 parent-pin task, a B33 advance or C1 reconciliation.

## 5. Repair qualification and retained ceiling

Run the original normal failing event-ID regression unchanged: both nonempty
claim/answer delta event IDs must reference the actual held epoch event.
Verify real attempt/source IDs and reason unchanged, mismatching epoch/runtime
event authority fails, full native prepared/finalized output stays exact, and
retained replay preserves original bytes. Extend tests only within the original
thirteen paths; no xfail/deselection or zero-delta-only substitute.

Continue ALL original C2 gates, including genuine commits/reconnects, exact
accounting, race/rollback matrices and independent full recomputation. Keep
D24 completion public_delta_count zero while validating exact nonempty D25
transition-history rows; seal owns final publication-child cardinality.

This docs amendment supplies no new executable qualification. Current held
checkpoint had798 pure passes, one live trace pass and one live event-ID
regression FAIL; earlier exploratory D28 run was interrupted after33 passes,
not full/current qualification. No source candidate has been accepted/pushed.
D34/D25 implementation, private C2, full B33/P33/C1, Task2/Task17, M5.4--M5.6,
deployment, AI quality and end-to-end utility/speed/cost remain PENDING.
Runtime remains v1_only outside isolated fixtures.
