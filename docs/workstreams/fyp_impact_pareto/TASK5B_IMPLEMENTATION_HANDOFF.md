# Task 5B Implementation Handoff

## Status

Task 5B is implemented on `codex/fyp-impact-pareto`. Its offline error-analysis,
retrospective Pareto, and hosted-request freeze gates pass. Task 5 overall is
not complete until Task 5C executes the staged hosted-model experiment and
records the neural-quality, effect-retention, time, and actual-cost evidence.

No M4 or M5 contract or milestone status changes.

## Result

- Task 4 oracle-positive misses explained: 8/8.
- Task 4 frontier pair attempts: 8; exhaustive pair attempts: 8.
- Retrospective held-out points: 24 (three policies, eight budgets).
- `old_new_rarity_coverage`, budget 8: 249/256 affected claims,
  124/128 fully covered events, 1,024/32,768 pairs.
- `old_new_rarity_coverage`, budget 64: 256/256 affected claims,
  128/128 fully covered events, 8,192/32,768 pairs.
- Frozen hosted population: 65,536 requests in four Batch parts.
- Estimated standard/Batch upper costs: USD 2.513623 / USD 1.256812.
- Report manifest hash:
  `ef826f2a4872f43828d639f30d68a354d206a22720f72b6b431bf33ced142f6a`.
- Hosted request manifest hash:
  `ad962ba1ec4336fd24f1d255f511cc93f5baa4a157d85d3c0f16e31a41d49bc2`.

## Reproduction artifact hashes

- `impact_pareto_report.json`:
  `524af3b130251b96a3c0d3642e0c6edf527849136245287b14fc62dbbe9b826e`
- `task4_misses.csv`:
  `1922f880d2372cebe40a34809dc28e8bfebd515dbf07bc63896333e62bfbeb92`
- `heldout_pareto.csv`:
  `e91d9e4143cca85bda85b9356c72620afd9e15cb8866782e59ff00061852169d`
- `request_manifest.json`:
  `c1af429ad1f16edd13b05e3ccc83252fe2586a3007d53df7c98dd64ab4cc28ad`
- `request_mapping.jsonl`:
  `56c4c3b633a3890d4f1f74af9e8ac95d32c4ba517ff5b86dcd5243b397b5b1ef`
- `selected_new_requests.jsonl`:
  `2268aae3cb0f4edd0c1c4d324316f672048082daff0e43dbb70b424d1f9472cb`
- Batch parts 00--03:
  `feb4b76d2f12f63c42a66eadaba6b4212d0d31bc880e6d067d83c1262cfeb449`,
  `bea9f587e0a9f212e173ee13302cf93bba7c4f734f6ff99b6648c5b94a5a6d9e`,
  `83a7583d19dbf219eb1221e48296ba91f2bbf858326822efabdce8c87b56520a`,
  `d1ddf26098991115745d589cf710766ccff2dd1ee14e524fee6f5a031f292f1b`.

## Verification evidence

```bash
python -m ruff check src/groundloop/hosted_verifier.py \
  src/groundloop/fyp_impact_pareto.py src/groundloop/cli.py \
  tests/m4/fyp_impact_pareto
python -m mypy src/groundloop/hosted_verifier.py \
  src/groundloop/fyp_impact_pareto.py
python -m pytest -q tests/m4/fyp_impact_pareto
python -m compileall -q src tests/m4/fyp_impact_pareto
```

Evidence at handoff: ruff `PASS`, strict mypy `PASS`, compileall `PASS`, and
`5 passed`. A real CLI rerun reproduced the two manifest hashes above in
53.585 seconds of local request-generation work. This is generation time, not
model or update latency.

## Next activation

Implement the fake-provider/parser gates, then run the capped 32-request
development smoke. Do not submit the held-out Batch population unless the
smoke has complete schema-valid responses and projected cumulative spend stays
below USD 4.00. Load the API key only from the ignored local environment and
never place it in artifacts, logs, tests, commits, or prompts.
