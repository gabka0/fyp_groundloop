# Task 5A Implementation Handoff

## Status

Task 5A is implemented in the isolated `codex/fyp-impact-selection` worktree.
The bounded held-out impact-selection checkpoint verdict is `PASS`.

Task 5 as a whole remains incomplete. Task 4 miss analysis, integrated
pair/claim/status/answer-effect Pareto measurement, actual model time, total
update time, batch calls, and provider cost remain for the next activation.

This does not change M4 or M5 milestone status and does not establish hosted
verifier accuracy, wall-clock speedup, cost savings, natural-history utility,
or end-to-end answer quality.

## Frozen inputs

- Config: `configs/fyp/impact_selection_v1.json`
- Config SHA-256:
  `4fcd68851f49f14ac068239cee8f24ba5eecc48e02538f68918e4773292c40da`
- Prepared VitaminC source SHA-256:
  `1a306620c363d52c6abfcdf5f6d272e7767cfdc8cbd364768b27901870a3c882`
- Source population: 1,024 rows, 256 cases, two 128-case strata
- Split: 64 development and 64 evaluation cases per stratum

## Result

- Selected policy: `old_new_rarity_coverage`
- Selected budget: 8 of 256 candidate claims per event
- Held-out affected-claim recall: 249/256 (97.3%)
- Held-out full-event coverage: 124/128 (96.9%)
- Selected pairs: 1,024
- Exhaustive pairs: 32,768
- Avoided pairs: 31,744/32,768 (96.9%)
- Report manifest hash:
  `dac99fd0ffff4749b6defe6ad3975e593f4615d6a291050ce0920da875bea9fa`
- Canonical report file SHA-256:
  `e74865979d17f9d7b64b6b2e20f983798230090ce9de122122e72030524a1e6f`
- Development CSV SHA-256:
  `948b1830d1613b1963898cb375a4472ba60ad60080e6573516a29d98c59a2f13`
- Evaluation-event CSV SHA-256:
  `94dfc18436e480c1000f4f98b068b65cbd19e68a4ce676064e39174e6ae55d81`

## Development chronology

The first two policies, new-evidence overlap and changed-token overlap,
returned `NO_CANDIDATE` on development; no held-out labels were evaluated.
The rarity-weighted old/new policy was then added because old-side text is
needed to retrieve claims invalidated by a revision. It passed development at
budgets 8, 16, and 32. The frozen smallest-budget rule selected 8, after which
that one candidate was run on the held-out half.

This is development-set iteration rather than an untouched preregistration.

## Verification commands

```bash
python3 -m ruff check src/groundloop/fyp_impact_selection.py \
  src/groundloop/cli.py tests/m4/fyp_impact_selection
python3 -m mypy src/groundloop
python3 -m pytest tests/m4/fyp_impact_selection -q
python3 -m compileall src tests
```

The final handoff must be updated if these gates or hashes change.

## Verification evidence

- Ruff over the full repository: `PASS`.
- Strict mypy over 152 source files: `PASS`.
- Compileall over `src` and `tests`: `PASS`.
- Task 5A plus Task 4 regression tests: `13 passed`.
- Real Task 5A CLI rerun: `PASS`, with the same report manifest hash recorded
  above.
- The ordinary one-shot full pytest command cannot currently collect the
  repository because separate pre-existing M5 test directories contain the
  same unqualified module names. No unrelated packaging was changed in this
  path-exclusive checkpoint.
- Segmented full-suite evidence: the main segment reached `1,212 passed`,
  `1,402 skipped`, and one environment-specific failure; the isolated M5
  runtime segment recorded `511 passed`; the isolated D24 requirement segment
  recorded `171 skipped`. The one failure is the pre-existing M4.10 pinned-Git
  source check: local `/home/kassym/dynagox` has no remote matching the frozen
  `https://github.com/gabka0/dynagox.git` identity. It is unrelated to Task 5.

## Next boundary

Continue the broader Task 5 program on a separate path-exclusive activation:
first produce the Task 4 miss taxonomy, then adapt the selector to the
GroundLoop policy/effect harness and execute the offline Pareto frontier.
Only after those bytes are frozen should the provider-neutral hosted-verifier
experiment measure label accuracy, latency, tokens, price, failures, status
effects, and answer effects. API keys must be loaded only from an ignored local
`.env`; no key may enter configuration, logs, fixtures, reports, commits, or
prompts.
