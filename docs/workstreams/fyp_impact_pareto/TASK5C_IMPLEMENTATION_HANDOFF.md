# Task 5C Hosted-Verifier Implementation Handoff

Status: **complete; pre-registered verdict `NO_GO`**

Execution completed: 2026-10-04

Branch: `codex/fyp-impact-pareto`

Canonical local artifact root:
`artifacts/fyp_impact_pareto/task5c/`

## 1. Outcome

The complete frozen hosted population executed with OpenAI `gpt-6-luna`,
reasoning `none`, and the strict one-field
`support|refute|neutral` schema. All 66,560 paid Batch requests returned one
valid response: 1,024 selected-new requests, 32,768 baseline-old requests, and
32,768 exhaustive-new requests. The selected-new role intentionally duplicates
a subset of exhaustive-new requests so its actual provider lifecycle and cost
can be measured independently.

No frozen budget met both clauses of the pre-registered gate:

- verifier-pair reduction at least 80%; and
- pair, claim, status, and answer-effect recall each at least 95%.

The machine verdict is therefore **`NO_GO`**. The current selector and hosted
verifier are not promoted into the application runtime.

## 2. Frozen identities

- Model: `gpt-6-luna`
- Prompt hash:
  `f38392b2ec95b3b8a65ec380bc3196def6096d6e61da11b945e59f6f9a94be6f`
- Request manifest hash:
  `ad962ba1ec4336fd24f1d255f511cc93f5baa4a157d85d3c0f16e31a41d49bc2`
- Source SHA-256:
  `1a306620c363d52c6abfcdf5f6d272e7767cfdc8cbd364768b27901870a3c882`
- Task 5B report hash:
  `ef826f2a4872f43828d639f30d68a354d206a22720f72b6b431bf33ced142f6a`
- Final report internal hash:
  `b646f8b854c7bc47d90e8cedf89f80319efcd371bc1a2b8a77d37fa27a1514c2`
- Final report file SHA-256:
  `e1b73eef29da579cb6f058e24e86bcd2db637b0e795df1b7a4524805fece495f`
- Baseline-old result SHA-256:
  `eeeed6ef5426c0a860b3fd0dd151ddb0b545a81571eb5c031e044678ca2e4e74`
- Exhaustive-new result SHA-256:
  `c7898ce2dc593510c3a003640c032c31eb32858914a5d3d72380555e29373ca4`
- Selected-new result SHA-256:
  `c484bbd0cb3ff5cf5e5499da1b24d531366739b69074f2e7b9a6bdb2ea8ed277`

The API key was read only from `OPENAI_API_KEY`. It is absent from request,
result, ledger, documentation, test, and Git artifacts.

## 3. Accuracy diagnostic

Gold joins are possible only for the 512 own-page endpoints in the exhaustive
population. They are a diagnostic, not the effect oracle.

| Surface | Correct | Accuracy |
|---|---:|---:|
| All own-page endpoints | 421/512 | 82.23% |
| Old evidence | 205/256 | 80.08% |
| New evidence | 216/256 | 84.38% |
| `support_neutral` | 215/256 | 83.98% |
| `support_refute` | 206/256 | 80.47% |
| Selected affected new endpoints | 211/249 | 84.74% |

The selected denominator is 249, not 256, because the frozen budget-8
selector includes 249 of the fixture-authored affected claims. Accuracy does
not cover arbitrary cross-page candidate pairs because they have no human gold
label.

This result confirms that replacing MiniLM with a stronger hosted model helps
the bounded quality surface, but does not make the semantic layer reliably
correct. The API remains a versioned model-observation producer, not an
objective-truth oracle.

## 4. Model-relative effect frontier

Exhaustive old/new judgments define the frozen model-relative effect oracle.
There are 336 changed pair/claim statuses and 138 changed controlled-answer
statuses.

| Budget | Pairs | Reduction | Pair/claim | Status | Answer |
|---:|---:|---:|---:|---:|---:|
| 1 | 128 | 99.61% | 110/336 | 210/336 | 74/138 |
| 2 | 256 | 99.22% | 209/336 | 283/336 | 99/138 |
| 4 | 512 | 98.44% | 221/336 | 291/336 | 103/138 |
| 8 | 1,024 | 96.88% | 238/336 | 297/336 | 106/138 |
| 16 | 2,048 | 93.75% | 268/336 | 310/336 | 114/138 |
| 32 | 4,096 | 87.50% | 289/336 | 320/336 | 123/138 |
| 64 | 8,192 | 75.00% | 317/336 | 329/336 | 132/138 |
| 256 | 32,768 | 0.00% | 336/336 | 336/336 | 138/138 |

Budget 32 satisfies the work clause but reaches only 86.01% pair/claim recall
and 89.13% answer recall. Budget 64 reaches 97.92% status recall and 95.65%
answer recall, but pair/claim recall is 94.35% and work reduction is only 75%.
No measured budget qualifies.

The key scientific finding is that Task 5A's fixture-authored affected set was
too narrow for the hosted model. It contained 256 affected claims, whereas
exhaustive hosted inference produced 336 model-relative changes across the
registry. The selector recovered textually intended changes well, but missed
enough additional model-behavior changes to fail the actual effect gate.

## 5. Work, cost, and timing

| Role | Requests | Calls | Input tokens | Output tokens | Batch cost | Processing | Submit-to-complete |
|---|---:|---:|---:|---:|---:|---:|---:|
| Selected new | 1,024 | 1 | 232,065 | 18,501 | $0.01622850 | 1,070 s | 1,132 s |
| Baseline old | 32,768 | 2 | 6,430,976 | 590,563 | $0.46918955 | 9,105 s | 9,177 s |
| Exhaustive new | 32,768 | 6 | 7,418,112 | 590,130 | $0.51843810 | 11,129 s | 11,206 s |

The nine Batch calls cost `$1.00385615`. The one allowed 32-request
standard-endpoint smoke cost `$0.00100570`, for total Task 5C spend of
`$1.00486185`, below the frozen `$4.00` cap.

For the selected update, selection took 1,630 ms, provider lifecycle took
1,132,000 ms, and local state projection took 1,776 ms, totaling 1,135,406 ms.
For exhaustive new re-verification, provider lifecycle took 11,206,000 ms and
projection took 1,775 ms, totaling 11,207,775 ms. The selected path was 89.9%
lower on this observed composite time.

That difference is operational evidence for this run only. The roles ran at
different times as asynchronous Batch work, exhaustive-new was divided into
six serial jobs because of the live queued-token cap, and the API exposes no
pure accelerator inference time. These numbers are not online latency or a
general GroundLoop speedup claim.

## 6. Recovery audit

The original exhaustive-new job failed provider validation with
`token_limit_exceeded`. The reported organization limit was 2,000,000 enqueued
input tokens. Provider counters were total 0, completed 0, failed 0 and there
was no output file, so no inference or result had occurred.

Before observing any exhaustive-new output, the exact ordered 32,768 request
identities were repartitioned under a 1,500,000 estimated-token cap into six
parts containing 6,258, 6,223, 6,128, 6,467, 6,285, and 1,407 requests. Every
part completed with zero failed requests. The recovery changed only the number
of transport jobs; model, prompt, bodies, identities, metrics, thresholds, and
spend cap remained fixed.

## 7. Evidence boundary

Task 5 is complete as a bounded negative experiment. It supplies:

- exact Task 4 miss explanations;
- a diff-aware, label-blind selector;
- an eight-budget retrospective sweep;
- held-out comparison with exhaustive hosted re-verification;
- verifier-pair, Batch-call, token, cost, provider-lifecycle, total-update,
  pair, claim, status, and answer-effect measurements; and
- a frozen simultaneous work/recall verdict.

It does not establish objective truth, representative natural-history utility,
online service latency, pure model-compute time, or a general speedup. It does
not alter the frozen M3 default verifier, M4 closure, M5 contracts, deployment
state, or any pending M5 implementation claim.

## 8. Validation

- Task 5 selector, Pareto, and hosted-runner tests: 15 passed.
- Canonical repository suite with the one unrelated pinned-remote identity
  assertion deselected: 3,305 tests executed, 1,732 passed, 1,573 declared
  skips, zero failures, and zero errors in 247.946 seconds.
- Ruff lint: passed.
- Strict mypy: passed over 154 source files.
- Cache-isolated `compileall` over `src` and `tests`: passed.
- `git diff --check`: passed.

The non-deselected repository run had one failure outside Task 5. The local
`/home/kassym/dynagox` checkout exists, so the M4.10 source test activates, but
its remotes are currently `git@github.com:gabka0/clone_dynagox.git` and
`git@github.com:NTU-DBT/dynagox.git`; neither exactly matches the frozen
`https://github.com/gabka0/dynagox.git` identity. The test therefore fails
before its content checks. No remote was added or changed for this Task 5
work.

The optional repository-wide Ruff formatter check is not a clean gate: it
reports 93 existing Python files that it would rewrite. Ruff lint is clean,
and this result does not perform that unrelated broad formatting change.
