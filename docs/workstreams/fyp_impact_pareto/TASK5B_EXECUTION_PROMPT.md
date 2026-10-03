# Task 5B Execution Prompt

Act as the GroundLoop Task 5B error-analysis and offline-Pareto agent.

Work only in `codex/fyp-impact-pareto` and obey `AGENTS.md`. Treat Task 4,
Task 5A, M4, M5, the prepared VitaminC JSONL, and prior result artifacts as
read-only evidence.

Implement `groundloop fyp-impact-pareto` exactly from `TASK5B_PLAN.md`.
Rerun the frozen Task 4 controlled evaluation and derive one row for every
oracle-positive miss without copying expected rows into the implementation.
Bind each explanation to channel ranks, configured budgets, admitted work and
all four event metric counts. Fail closed for an unrecognized fixture shape.

Rerun the Task 5A split and emit the complete retrospective held-out frontier
for all three policies and budgets 1, 2, 4, 8, 16, 32, 64 and 256. Determine
Pareto optimality from exact integer comparisons, not floats.

Prepare the hosted-verifier request population with stable identities and a
strict schema, but do not read an API key or make a network call in Task 5B.
Emit canonical JSON, miss CSV, Pareto CSV, request-manifest JSON and request
JSONL. Add fail-closed mutation, leakage-boundary, determinism, CLI, lint,
strict-type and regression tests.

Do not change frozen runners, use oracle labels in ranking/request creation,
claim real-model accuracy, claim wall-clock speedup, spend API credit, or
change M4/M5 status.
