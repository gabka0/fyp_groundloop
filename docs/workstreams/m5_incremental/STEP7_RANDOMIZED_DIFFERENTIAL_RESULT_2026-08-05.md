# M5 incremental overlay Step 7 randomized differential result

Status: candidate-local PASS; not integrated and not an M5-complete claim

Date: 2026-08-05

Worktree: `/home/kassym/Desktop/groundloop-worktrees/m5-full-overlay`

Branch: `workstream/m5-full-overlay`

Accepted checkpoint: `38ea5b6` (`Correct M5 certificate identity oracle`)

## Verdict

The frozen seed-`20260802` randomized differential gate completed with
100,000 newly committed events and zero mismatches. Exact replay and generator
rejection paths were checked separately and did not count toward the committed
event target. The result was verified against the checked-in `planned_full`
manifest section.

This is local in-memory M5.2/Step 7 evidence relative to stored observations,
the frozen policy semantics, and the independent Python reference checks. It
is not PostgreSQL third-oracle evidence, integration evidence, production
latency evidence, an M5.4/M5.5 result, or an M5-complete, security, neural
quality, novelty, or production-readiness claim.

## Frozen inputs

| Artifact | SHA-256 |
| --- | --- |
| raw config | `54f13958040f8480e5b680804385884f3568eef38f5732d8b9f97522a2972041` |
| canonical config | `5e33af72f5a54d0d17d3fbb176979268f4e760d98512acc5d95ec1a1561eab8e` |
| event weights | `d0f910c164294d0a4574bbca01b181671727957c7f279533e8ded5e65fb91425` |
| runner | `f1d68d25ef07ac7de8452ef376bf4815a020999e91dadf6a5adcc6133665cafa` |
| raw manifest | `ea6b6b7e43e8ca39c955b4b35d051de329e12fee2940c5d541a39d37dd61e7a9` |
| canonical manifest | `e98b276043340a43ea13fd3f949aecfedf7df83474f7fe293cf77d45b6d230c5` |
| focused test file | `bd228dd935574364b043bcb53e18ad4bd852fd8ba34380408e5c9fb0c8242a30` |

The manifest fixes 1,000 streams with 100 commits per stream, at most 400
proposals per stream, and a 250-commit short prefix. Its planned stream hashes
are:

- proposals: `8de8bc34f5051a3d4ab3816e128bc3b58c79634a223346a4a9a483cadaeb17f2`;
- committed events: `f7f2b62e14ba6edcfec4e56bc6788b9b56e18e89d6449b5022be13350a33b8b7`;
- fixtures: `45773742eed7c43d50f6fb842d219106588649ca30b38652fe67de3fd6a46da0`.

## Accepted execution

Gate command (external `/usr/bin/time` timing wrapper omitted):

```bash
PYTHONPATH=src:tests:experiments/streams \
  /home/kassym/Desktop/groundloop/.venv/bin/python \
  experiments/streams/run_m5_differential.py \
  --mode full \
  --output results/m5/m5_incremental_differential_seed_20260802_full.json
```

The local result is ignored by Git as required. Its SHA-256 is
`3137be7f63b3608afb2e5658616ed536bef043b259eacf3afa8ab4973d5ac0c4`.
The execution trace SHA-256 is
`1f25f92b0f9aa6def4d619bffb738f2c1eeeae624a912ed9b11a54c964b44876`.

| Measure | Accepted result |
| --- | ---: |
| committed events | 100,000 |
| proposals | 128,457 |
| exact replays | 7,731 |
| generator rejections | 20,726 |
| state checks | 100,000 |
| replay checks | 7,731 |
| generator-rejection no-op checks | 20,726 |
| shard audits | 1,000 |
| matching-index audits | 1,000 |
| certificate-history audits | 1,000 |
| current-certificate checks | 315,570 |
| public-certificate checks | 6,376 |
| mismatches | 0 |

The result JSON reports 1,022.259 seconds for the validated phase and 97.823
committed events per second. Terminal-session observations recorded 1,039.09
seconds of whole-command wall time, including planning and result publication,
and 30,180 KiB maximum RSS from `/usr/bin/time`; no separate durable timing log
was retained. These are local harness measurements, not production latency
claims.

All nine committed event classes and all thirteen proposal kinds occurred.
The run generated 28,686 duplicate chunks. The largest stream required 284
proposals, below the frozen cap of 400.

## Executed bounds

The harness enforced every frozen runtime cap on every applicable event.

| Quantity | Frozen cap | Maximum observed |
| --- | ---: | ---: |
| active groups | 6 | 6 |
| active chunks | 16 | 16 |
| transaction observation population, `N_obs` | 288 | 18 |
| requirements, `r` | 3 | 3 |
| group observation population, `W_g` | 48 | 12 |
| distinct group edges, `E_g` | 18 | 6 |
| distinct group hashes, `H_g` | 6 | 5 |
| reference matching leaf bound | 2,058 | enforced before evaluation |

The run also audited 24,090 structural group versions. Its maximum paired
structural population was `W=10`, `E=5`, and `H=4`.

## Attempt history and oracle correction

No failed or interrupted run is counted as accepted evidence.

1. The first pre-hardening attempt was deliberately interrupted before result
   publication after a late read-only audit found certificate/history and
   replay-oracle gaps. The terminal session recorded exit 130; no result file
   was produced.
2. The first committed hardened attempt at `1949abd` stopped at global
   committed event 22,767 with `group artifact transition differs from
   independent stateful reconstruction`. The terminal session recorded 249.20
   seconds elapsed; no result file was produced. Independent reproduction
   showed an oracle overconstraint, not an overlay mismatch: a policy change
   made an ambiguous two-requirement graph complete, and the incremental
   ordered-mask constructor and exhaustive reference constructor selected
   different publicly valid perfect matchings.
3. Commit `38ea5b6` corrected only that cross-oracle identity assumption,
   consistent with M5-D9: independent oracles compare certificate validity,
   not alternate valid matching identity. RETAIN, untouched-key, same-edge
   least-observation REPAIR, and policy-only rebinds whose selected edges
   survive remain exact; binding/history, publication, ledger, replay, policy
   identity, public-validity, and work checks also remain exact. BUILD and any
   independently detected selected-edge-loss REBUILD, including rebind
   rebuilds, allow alternate matching identity subject to independent
   current-witness validity. Shard audits also exercise the public validators.
4. Before refreezing the manifest, a 30,000-commit diagnostic crossed event
   22,767 and completed with 30,000 state checks, 2,307 replay checks, 6,391
   rejection checks, 300 shard/index/history audits, and zero mismatches. This
   bounded diagnostic is supporting evidence, not the accepted long result.

The accepted 100,000-event execution was then run from the clean, committed
`38ea5b6` checkpoint against the regenerated manifest.

## Supporting gates

Before the accepted long run:

- the complete focused randomized gate passed 13 tests with one intentional
  opt-in full-run skip;
- the 250- and 1,000-commit validated prefixes passed with zero mismatches;
- four `PYTHONHASHSEED` variants produced the same frozen prefix summary;
- Ruff lint, Step 7-owned Python format checks, strict mypy, compileall, and
  `git diff --check` passed;
- three independent read-only reviews approved the exact runner semantics,
  and two independently regenerated the final manifest/file hashes.

The result JSON was reloaded after execution and passed
`verify_manifest_header(...)` and
`verify_manifest_summary(..., "planned_full")`.

## Remaining barrier

This document closes only the overlay candidate's local Step 7 randomized
gate. The candidate still requires final full-tree regression/static audit and
the restart handoff's sequential integration gates. The frozen PostgreSQL
candidate must be integrated and validated first, followed by this overlay
candidate and integrated three-oracle work. Main status/acceptance documents
must be reconciled only after that evidence exists. M5.4 and later stages
remain pending.
