# M5-D30 Lane-P Custody-Evidence Correction

Status: docs-only corrective-authority candidate; it authorizes no source,
test, handoff, runtime-mode, implementation-verdict, next-lane, deployment,
performance, utility, security, novelty, or AI-quality claim until these exact
bytes receive two independent identical-byte `GO`, `P0=0`, `P1=0` reviews,
are committed as the sole changed path, receive two independent postcommit
identity confirmations, and are pushed to this branch and `origin/main`

Date: 2026-09-24

## 1. Exact authority barrier

```text
required_parent = 55a4791497d31d98d2c044ac61f217f49f13dd29
required_parent_tree = abd373a6657f581d08f8aead70ed9cf85afc396e
required_origin_main = 55a4791497d31d98d2c044ac61f217f49f13dd29

d30_sequencing_amendment_commit = dc8601707629dd313a5d1d41d0adb84c5acc810e
d30_sequencing_amendment_sha256 = df06b98fc876d20dd66d94ac61db051d85a96867f7d4c8d542b5b915d6d79512
d30_packaging_authority_commit = 55a4791497d31d98d2c044ac61f217f49f13dd29
d30_packaging_authority_sha256 = acbc323c5def2af45bf2fc0f82b77ebfb5553c9bd1457f27bba1be72f6be8d4b

candidate_branch = workstream/m5-d30-lanep-custody-evidence-correction
candidate_worktree = /home/kassym/Desktop/groundloop-worktrees/m5-d30-lanep-custody-evidence-correction
candidate_changed_path_count = 1
runtime_mode = v1_only
```

The sole owned path before acceptance is this file. Its commit must be a
one-file linear child of the exact pushed packaging-authority commit. Any
other path, parent or intervening commit restarts both reviews. The active
dirty Lane-P worktree, protected local main, source, tests, `.gitignore`,
`pyproject.toml`, migrations and generated artifacts remain read-only during
this docs gate.

## 2. Confirmed custody-evidence defect

The accepted Lane-P evidence-sequencing amendment required the coordinator,
before its one-time dirty-lane fast-forward, to record both the raw
NUL-delimited `git status --porcelain=v2 -z --untracked-files=all` bytes and
their SHA-256. The contemporaneous transcript recorded all of the following:

1. pre/post branch, HEAD, tree and empty-index identity;
2. raw-status record count `23` and SHA-256
   `5a8f76a40a7c692acd421d636d525fb81ca2c10c71ccdff7de0d45b14aa0bb16`;
3. a complete 23-row decoded path/kind/base-index-mode-or-absence/
   prospective-mode/filesystem-mode/byte-count/content-SHA ledger;
4. byte-identical 5,293-byte pre/post `PATH_LEDGER` blocks with SHA-256
   `3bd2d8b33a1a31aa696875083e8c39112e7a819720b3c6ecdf62590e88edca96`;
5. the sole disjoint intervening docs path and protected-checkout identity;
   and
6. post-fast-forward equality for every decoded row, the separately computed
   record count and the raw-status SHA-256 commitment.

The transcript did not preserve a copy or encoding of the raw status bytes or
emit their 2,001-byte length. Therefore the temporal instruction to record the
raw bytes was not literally satisfied, even though their contemporaneous
digest and complete decoded state were retained. The Lane-P handoff disclosed
this gap; it must not be erased, reworded as contemporaneous retention, or
converted to an unconditional sequencing-gate pass. The byte length below is
established only by the later recovered preimage.

This is an evidence-custody defect, not evidence of a source, test, mode,
schema, migration, digest, result-byte or runtime mutation. It nevertheless
blocks Lane-P acceptance until corrected under explicit authority.

## 3. Exact raw-byte recovery

On 2026-09-24 the coordinator created a fresh local clone, checked out the
immutable pre-fast-forward commit
`c7518557f9c6f957a6e38f13930f55395a95f728`, and recreated exactly the 23
recorded path statuses. The five tracked paths used that commit's exact
base/index blobs and remained modified; the eighteen recorded untracked paths
were present. Worktree content bytes do not enter porcelain-v2 records, while
their independently retained historical sizes and SHA-256 values remain in
the contemporaneous ledger. The exact recovery command was:

```text
git -C <fresh-clone> status --porcelain=v2 -z --untracked-files=all
```

The recovered stream contains 2,001 bytes and 23 NUL-terminated records. Its
SHA-256 is exactly the contemporaneous pre/post commitment:

```text
5a8f76a40a7c692acd421d636d525fb81ca2c10c71ccdff7de0d45b14aa0bb16
```

The following RFC 4648 Base64 text is the complete recovered raw stream. It
contains 2,668 ASCII characters and has SHA-256
`b64e08a8cc6425328b1c3b7053b3a843c4c19a979fc45a8f128e68565dcb1e47`:

```text
MSAuTSBOLi4uIDEwMDY0NCAxMDA2NDQgMTAwNjQ0IDgzM2EzYjJmMWU4ZDliMTExNzE5N2MyYWNlMDQyNjJjZjJhYTk3MDYgODMzYTNiMmYxZThkOWIxMTE3MTk3YzJhY2UwNDI2MmNmMmFhOTcwNiBzcmMvZ3JvdW5kbG9vcC9tNS9ydW50aW1lL3Bvc3RncmVzX21hdGNoaW5nLnB5ADEgLk0gTi4uLiAxMDA2NDQgMTAwNjQ0IDEwMDY0NCAwNDhjMDA3NzhjZTJiZmMyYWYxOTc0MDEwZGI3ZDdhM2VkNmVhNmE2IDA0OGMwMDc3OGNlMmJmYzJhZjE5NzQwMTBkYjdkN2EzZWQ2ZWE2YTYgdGVzdHMvbTUvcG9zdGdyZXNfcnVudGltZS9kMjVfc3RvcmVfY29yZS9jb25mdGVzdC5weQAxIC5NIE4uLi4gMTAwNjQ0IDEwMDY0NCAxMDA2NDQgYzIxYzdmNzg2ZjQ5YjkwYWZhOTI3OGI4ZDQ3OTY1YjRhOWVkNjVkMCBjMjFjN2Y3ODZmNDliOTBhZmE5Mjc4YjhkNDc5NjViNGE5ZWQ2NWQwIHRlc3RzL201L3Bvc3RncmVzX3J1bnRpbWUvZDI1X3N0b3JlX2NvcmUvdGVzdF9wb2ludHMucHkAMSAuTSBOLi4uIDEwMDY0NCAxMDA2NDQgMTAwNjQ0IGU0YTgzZTVjNzJhZWQyMDdlNjQxZGY1OGVhMjY3MjliOThhYTJiMWEgZTRhODNlNWM3MmFlZDIwN2U2NDFkZjU4ZWEyNjcyOWI5OGFhMmIxYSB0ZXN0cy9tNS9wb3N0Z3Jlc19ydW50aW1lL2QyNV9zdG9yZV9jb3JlL3Rlc3RfcmVwbGF5X3dvcmsucHkAMSAuTSBOLi4uIDEwMDY0NCAxMDA2NDQgMTAwNjQ0IDFhM2QzYTI2ZTNiZmVmZmI0MzJhMDQ3NDM5YzBiMjkwOTU1YTk1M2UgMWEzZDNhMjZlM2JmZWZmYjQzMmEwNDc0MzljMGIyOTA5NTVhOTUzZSB0ZXN0cy9tNS9wb3N0Z3Jlc19ydW50aW1lL2QyNV9zdG9yZV9jb3JlL3Rlc3RfdHJhbnNpdGlvbl9hcHBseS5weQA/IHNyYy9ncm91bmRsb29wL201L3J1bnRpbWUvcG9zdGdyZXNfd2l0aGRyYXdhbC5weQA/IHRlc3RzL201L3Bvc3RncmVzX3J1bnRpbWUvZDI1X3N0b3JlX2NvcmUvdGVzdF9kMjdfY291bnRlcl9vd25lcnNoaXAucHkAPyB0ZXN0cy9tNS9wb3N0Z3Jlc19ydW50aW1lL2QyNV9zdG9yZV9jb3JlL3Rlc3RfZDI4X3BoYXNlZF9jb21wb3NpdGlvbi5weQA/IHRlc3RzL201L3Bvc3RncmVzX3J1bnRpbWUvZDI1X3N0b3JlX2NvcmUvdGVzdF9ub25lbXB0eV9wbGFubmVyLnB5AD8gdGVzdHMvbTUvcG9zdGdyZXNfcnVudGltZS9kMjlfc3RvcmUvY29uZnRlc3QucHkAPyB0ZXN0cy9tNS9wb3N0Z3Jlc19ydW50aW1lL2QyOV9zdG9yZS90ZXN0X2JvdW5kZWRfd2l0aGRyYXdhbC5weQA/IHRlc3RzL201L3Bvc3RncmVzX3J1bnRpbWUvZDI5X3N0b3JlL3Rlc3RfcXVlcnlfcGxhbnMucHkAPyB0ZXN0cy9tNS9wb3N0Z3Jlc19ydW50aW1lL2QyOV9zdG9yZS90ZXN0X3JlcGxheV9hbmRfcmVzZXJ2YXRpb24ucHkAPyB0ZXN0cy9tNS9wb3N0Z3Jlc19ydW50aW1lL2QyOV9zdG9yZS90ZXN0X3JldGFpbmVkX2RlY2xhcmF0aW9ucy5weQA/IHRlc3RzL201L3Bvc3RncmVzX3J1bnRpbWUvZDI5X3N0b3JlL3Rlc3Rfc291cmNlX2lkZW50aXR5LnB5AD8gdGVzdHMvbTUvcG9zdGdyZXNfcnVudGltZS9kMzBfc3RvcmUvY29uZnRlc3QucHkAPyB0ZXN0cy9tNS9wb3N0Z3Jlc19ydW50aW1lL2QzMF9zdG9yZS90ZXN0X2FjdGl2YXRpb25fbTNfcHJvdmVuYW5jZS5weQA/IHRlc3RzL201L3Bvc3RncmVzX3J1bnRpbWUvZDMwX3N0b3JlL3Rlc3RfZHluYW1pY19jbGFpbV9wcm92ZW5hbmNlLnB5AD8gdGVzdHMvbTUvcG9zdGdyZXNfcnVudGltZS9kMzBfc3RvcmUvdGVzdF9vd25lcl90b3BvbG9neS5weQA/IHRlc3RzL201L3Bvc3RncmVzX3J1bnRpbWUvZDMwX3N0b3JlL3Rlc3RfcHJlZGVjZXNzb3JfcHJvYmUucHkAPyB0ZXN0cy9tNS9wb3N0Z3Jlc19ydW50aW1lL2QzMF9zdG9yZS90ZXN0X3F1ZXJ5X3BsYW5zX2FuZF9yYWNlcy5weQA/IHRlc3RzL201L3Bvc3RncmVzX3J1bnRpbWUvZDMwX3N0b3JlL3Rlc3RfcmVwbGF5X25vbmNoYW5nZS5weQA/IHRlc3RzL201L3Bvc3RncmVzX3J1bnRpbWUvZDMwX3N0b3JlL3Rlc3RfdG90YWxfY2xhaW1fY3VycmVuY3kucHkA
```

Independent reproduction must decode this block, prove exact byte/count/hash
identity, parse the 23 NUL records, and match every record to the immutable
`c7518557...` base/index identity and contemporaneous decoded ledger. A digest
match alone without decoding and row comparison is insufficient.

Because the contemporaneous SHA-256 is a binding commitment and the recovered
2,001-byte preimage matches it exactly, this recovery removes ambiguity about
the status stream's bytes, subject to the standard collision-resistance
assumption. It does not change the truthful fact that the byte artifact was
recovered after, not retained before, the original fast-forward.

## 4. Narrow corrective authority

After two independent reviewers reproduce every Section-3 assertion on these
exact candidate bytes, this amendment authorizes the combination of:

1. the contemporaneous pre/post SHA-256 and separately computed record count;
2. the complete contemporaneous decoded path/mode/content ledger and its
   independent transcript hashes; and
3. the later exact 2,001-byte raw preimage and Base64 encoding above

as the corrected evidence package for the sequencing-amendment fast-forward.
It supersedes only the sequencing amendment's unmet temporal raw-byte-copy
requirement. It does not rewrite the historical event, authorize a claim that
the raw file was retained contemporaneously, or waive any other custody,
implementation, testing, packaging, audit or claim-ceiling gate.

No source, test, `.gitignore`, migration, schema, installer, public API, digest,
counter, timing coordinate, result byte, model, prompt, provider, calibration,
deployment or runtime-mode change is authorized by this file.

## 5. One-time authority fast-forward for the active dirty lane

After these exact bytes are reviewed, committed, postcommit checked and pushed
to this branch and `origin/main`, the coordinator must stop all Lane-P writers
and record the active worktree/branch/HEAD/tree, empty staged delta, complete
raw porcelain-v2-z bytes/hash/count, all current path modes/sizes/SHA-256
values, exact protected-checkout identity, absence of writers, and a complete
parent-to-amendment delta proving this file is the sole disjoint path.

Only then may the active dirty worktree execute:

```text
git merge --ff-only <exact-pushed-correction-tip>
```

The post-fast-forward raw status bytes, path ledger, index and protected state
must remain identical. Any mismatch aborts. No merge commit, rebase, stash,
reset, checkout overwrite or file copy is authorized. Lane P then records this
correction's identities in its handoff. Every final Lane-P gate must execute on
one final candidate; pre-correction results may not be pooled into that final
evidence set merely because this correction is docs-only.

## 6. Claim ceiling

This correction does not accept Lane P. Even after it is integrated, the only
eventual permitted Lane-P labels, if all remaining gates pass, are exactly:

```text
matching_planner_repair = PASS
cross_layer_publication_races = DEFERRED_TO_LANE_D_AND_LANE_I
whole_route_output_nonchange = DEFERRED_TO_LANE_D_AND_LANE_I
M5-D30 implementation = PENDING
Task 2 = PENDING
runtime_mode = v1_only
```

M5-D24 through M5-D30 and M5.0-24 through M5.0-30 remain
`implementation-PENDING`. M5.4-05 through M5.4-09, M5.5, M5.6, deployment,
performance, utility, security, objective truth, novelty, maintained history,
named-system superiority and AI-quality claims remain `PENDING`.
