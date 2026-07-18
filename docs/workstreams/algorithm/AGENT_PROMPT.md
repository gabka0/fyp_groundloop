# Agent 3 Prompt — Algorithm Optimization and Formal Guarantees

Read `AGENTS.md`, `docs/agent_expert_operating_principles.md`, Sections 5, 7,
8, 11, and 14 of `docs/technical_design.md`, the database-theory section of
`docs/claude_algorithm_design_review.md`, and
`docs/parallel_agent_execution_plan.md` completely before acting.

You own only:

- `src/groundloop/optimized/**`
- `tests/optimized/**`
- `experiments/analysis/ivm/**`
- `experiments/streams/algorithm_*`
- `docs/theory/**`
- `docs/workstreams/algorithm/**`

Do not edit the current incremental engine, reference oracle, domain/events,
SQL lane, baseline lane, technical design, decisions, or dependency manifest.
Request shared changes through `docs/workstreams/algorithm/contract_requests/`.

## Objective

Produce one optimized, differentially verified algorithm whose mathematical
guarantee is precise, honest, and matched by code. Determine from primary
sources whether it is novel, a specialization, or known technique.

## Step 1 — formal model

Define the maintained direct-witness view and parameters:

- `E`: active current observations;
- `C`: claims;
- `A`: answers;
- `k`: observations withdrawn by one document update;
- `f`: observation labels flipped by a policy update;
- `p`: downstream claim/answer boundary changes;
- `d`: distinct contributing content hashes for a touched claim.

State the RAM/index assumptions and distinguish expected hash-map bounds from
worst-case ordered-map bounds. Exclude neural inference only when explicitly
stated.

## Step 2 — prove current semantics

Write invariants and proofs for signed content refcounts, distinct-hash
zero-crossings, score maxima, supersession, claim states, answer propagation,
and certificate validity. This proof is required even if the optimization later
fails.

## Step 3 — exact-flip policy index

Under frozen tie rule v1, derive and prove the partition:

```text
P_support = s > r and s > n
P_refute  = r >= s and r >= n
```

Show that threshold changes can only alter labels inside the corresponding
potential set. Design ordered indexes that enumerate exact flips for one or
both threshold changes.

Target theorem, subject to proof:

```text
space: O(E)
observation point update: O(log E) index work
threshold-only policy update: O(log E + f + p)
lower bound for explicit state maintenance: Omega(f + p)
```

State precisely whether two `O(log E)` searches are compressed in notation.
Handle equality boundaries and the REFUTE-conservative tie rule exhaustively.

## Step 4 — implement independently

Implement the candidate under `src/groundloop/optimized/`, not by modifying the
existing engine. Reuse immutable domain records but not reference computation as
maintenance logic. Add table-driven threshold-boundary tests, randomized
differential tests, and certificate checks.

## Step 5 — adversarial complexity tests

Generate:

- many interval scores that can never win because neutral dominates;
- `f = 0`, sparse `f`, and `f = Theta(E)` policy updates;
- high duplicate-content multiplicity;
- supersession storms;
- `k = Theta(E)` document withdrawal.

Instrument visited candidates. Demonstrate that the exact-flip index visits
`f`, not the raw threshold-interval population, when the partition is selective.

## Step 6 — literature audit

Use primary sources and inspect at least DBSP, F-IVM, CROWN, classical counting
IVM, dynamic conjunctive-query maintenance, and indexed selection/predicate
maintenance. Search specifically for parameter/threshold updates and
output-sensitive view maintenance. Record search terms, dates, theorem
assumptions, and direct links.

Classify the result as exactly one of:

- novel theorem/mechanism;
- known mechanism specialized to GroundLoop;
- engineering optimization only;
- disproved or not useful.

Default to the weaker classification when evidence is incomplete.

## Step 7 — handoff

Provide theorem statements, proofs, counterexamples, complexity table,
differential results, benchmark scripts, primary-source positioning, and an
explicit confidence level. Commit, rebase on `main`, rerun all owned tests, and
complete `HANDOFF.md`.

Do not write “faster than existing works” unless an apples-to-apples comparison
supports it. A valid and likely result is “output-sensitive and asymptotically
faster than full relabeling when `f = o(E)`, with worst-case degeneration to
linear work.”
