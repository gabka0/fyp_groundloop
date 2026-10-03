# Task 5A Execution Prompt

Act as the GroundLoop FYP Task 5A impact-selection implementation agent.

Work only in the isolated `codex/fyp-impact-selection` worktree and obey
`AGENTS.md`. Treat M3, M4, M5, the prepared M4.13 VitaminC artifact, and all
existing result files as read-only evidence.

Implement `groundloop fyp-impact-selection` exactly from `TASK5_PLAN.md`.
Fail closed unless the source SHA-256, row hashes, schema, 256-case population,
two 128-case strata, and four-row old/new case topology all match. Use only
claim text and old/new evidence text inside the ranking function. Never expose
gold labels, case identity, page identity, stratum, or expected affected
claims to that function.

Implement deterministic tokenization, multiset overlap, changed-token
extraction, and exact rarity-weighted old/new token coverage. Evaluate the
three frozen policy families and six frozen budgets on
the stratified development half. Apply the frozen eligibility thresholds and
tie-break rule, then evaluate only the selected candidate on the held-out
half. Preserve all metric numerators and denominators and compute pair-work
reduction from integer counts.

Emit byte-stable canonical JSON, development-frontier CSV, and held-out
event CSV. Bind the report to the exact source and config hashes. Add focused
unit, mutation, leakage-boundary, serialization, and CLI tests. Run lint,
strict type checking, compilation, focused tests, relevant regressions, and
the real bounded diagnostic.

Do not call a hosted or local neural model, tune after inspecting held-out
labels, modify the source artifact, claim wall-clock speedup, claim AI-quality
improvement, claim representative utility, or change M4/M5 status.
