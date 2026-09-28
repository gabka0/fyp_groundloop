# Task 4 Execution Prompt

Act as the GroundLoop FYP Task 4 value-benchmark implementation agent.

Work only in the isolated `codex/fyp-value-benchmark` worktree and obey
`AGENTS.md`. Treat the existing M4 controlled experiment, its five policies,
fixture, metrics, and report as frozen read-only evidence.

Implement `groundloop fyp-value-benchmark`. Reuse the frozen controlled runner
and aggregate actual work rather than configured budgets. Compare each policy
with the exhaustive claim-by-inserted-chunk verifier baseline. Preserve exact
integer metric counts for positive-pair, positive-claim, status-effect, and
answer-effect recall.

Define a fail-closed per-policy value gate: a policy qualifies only when it
uses strictly fewer verifier-pair attempts than exhaustive re-verification,
has no failed attempts, and has numerator equal to a positive denominator for
all four metrics. The overall result is `GO` only if at least one policy
qualifies; otherwise it is `NO_GO`.

Write a canonical JSON value report, one compact CSV policy table, the complete
underlying controlled report bundle, and a concise terminal summary. Bind the
derived report to the source controlled report, config, and fixture hashes.
Reject incomplete, duplicated, reordered, internally inconsistent, or
over-exhaustive inputs instead of silently normalizing them.

Add focused unit and CLI tests, including deterministic bytes and a mutation
test proving that reduced work alone cannot pass the gate. Update the README
and write a user guide and final handoff containing exact commands, hashes,
test counts, measured values, verdict, and limitations.

Do not add a new policy, tune the fixture, call a live model, claim speedup,
claim AI accuracy, claim empirical utility, or alter M4/M5 milestone status.
