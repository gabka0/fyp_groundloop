# M5.2 incremental overlay restart execution plan

Status: active implementation plan; no integration authorization

Date: 2026-08-05

Worktree: `/home/kassym/Desktop/groundloop-worktrees/m5-full-overlay`

Branch: `workstream/m5-full-overlay`

Preserved checkpoint: `ac6859f5b196b00e3cdeaba9f882fd52bdffc348`

## Barrier and ownership

The PostgreSQL candidate was finished and independently audited first. It is
frozen in its own worktree and is not merged here. Main remains out of scope,
including its user-owned `pyproject.toml` and `docs/presentations/` changes.

The coordinator is the only implementation owner for this restart lane:

- `src/groundloop/m5/incremental_overlay.py`;
- `src/groundloop/m5/claim_certificates.py`;
- focused files under `tests/m5/incremental/`;
- this plan and the eventual overlay handoff under
  `docs/workstreams/m5_incremental/`.

`src/groundloop/m5/__init__.py` is preserved at the checkpoint state and will
not be reconciled against main until both candidates have passed independent
audit. Other agents are read-only auditors and may not edit these paths.

### Step 7 ownership amendment

After Step 6 received independent read-only GO at commit `9b36664`, the
coordinator extends the same path-exclusive lane to the frozen randomized
differential gate. No other agent may edit these paths:

- `configs/m5/incremental_differential_v1.json`;
- `configs/m5/incremental_differential_seed_20260802_manifest_v1.json`;
- `experiments/streams/run_m5_differential.py`;
- `tests/m5/incremental/test_m5_randomized_differential.py`;
- `docs/workstreams/m5_incremental/STEP7_RANDOMIZED_DIFFERENTIAL_RESULT_2026-08-05.md`.

The full local result may be written under ignored `results/m5/`; it is not a
source artifact and will not be committed. The configuration, manifest, and
short deterministic prefix must be committed before the opt-in 100,000-event
run. The result document is added only after that frozen run completes.

## Small-commit sequence

1. Add regression scaffolding for forced two-child AVL deletion, O(1) linked
   history append/carry-forward, and the ban on measured `export_snapshot`.
2. Remove event-path global sorting through deterministic insertion-ordered
   propagation and freeze a new logical-output digest tag if byte order changes.
3. Add a selected-support claim-certificate transition helper while preserving
   the exhaustive public builder and validator.
4. Split claim-state dirtiness from claim-certificate dirtiness so a repair in
   a nonselected alternative cannot scan or republish unrelated claim support.
5. Classify artifact-, current-binding-, and history-only transitions before
   constructing changed and certificate-only result IDs.
6. Add per-event reference differential, failure/rollback/retry, exact/conflict
   replay, hash-seed, and every-term M5-T2 complexity gates.
7. Freeze the seed-`20260802` event manifest before running the opt-in
   100,000-committed-event gate; record rejected proposals separately.

Each semantic step receives focused tests and a separate commit. No checkpoint
is integrated to main until the complete overlay candidate receives an
independent read-only audit and the restart handoff's sequential merge gates
are ready.

## Claim boundary

This lane maintains exact state relative to stored observations and frozen
policies. Passing local differential or complexity gates is not an M5-complete,
production-readiness, neural-quality, or novelty result.
