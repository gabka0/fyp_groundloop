# GroundLoop Architecture

This is the short architectural overview. The frozen design containing the
formal schema, delta rules, consistency protocol, and scope decisions is
`docs/technical_design.md` (v0.2). Where the two differ, v0.2 governs.

## Design Principle

GroundLoop separates uncertain semantic computation from exact state
maintenance.

```text
Document updates
    -> versioned passages
    -> semantic impact selection
    -> retrieval and neural verification for selected delta pairs
    -> versioned semantic observations
    -> incremental relational propagation
    -> claim and answer status deltas
```

## Main Components

### 1. Versioned corpus store

Stores immutable document and passage versions. Replacement is represented as
deactivation of the old version followed by insertion of the new version.

### 2. Static RAG pipeline

Retrieves evidence, generates a cited answer, extracts atomic claims, and
records the generator, prompt, retriever, and extractor versions.

### 3. Claim and evidence registry

Stores claims as standing semantic objects and records candidate passages,
ranks, immutable scores, derived current decisions, and model versions.

### 4. Impact selector

Produces a conservative candidate set of old claims that may be affected by a
new or modified passage. Deletions use exact reverse dependencies; insertions
require approximate semantic discovery.

### 5. Neural verifier

Maps a claim and passage or bounded evidence group to versioned support,
refutation, and neutral scores. Verifier results are immutable observations.

### 6. Incremental maintenance engine

Maintains requirement satisfaction, evidence-group completeness, claim state,
and answer state. It emits explicit status deltas with provenance.

### 7. Full-recomputation reference engine

Evaluates the same relational semantics from all active stored observations.
It is the correctness oracle for differential tests.

### 8. Answer-health API and dashboard

Displays answers, claims, active evidence, status transitions, document diffs,
and neural calls avoided.

## Bounded Dependency Model

```text
PassageVersion
    -> Verification / RequirementWitness
        -> EvidenceRequirement
            -> EvidenceGroup
                -> Claim
                    -> Answer
```

No edge may point back to an earlier level. The FYP does not permit cycles or
unbounded recursive derivation.

## Maintained Semantics

Counts are over distinct normalized chunk content (`text_hash`), restricted to
*current* observations (one per subject-chunk-task key; new results supersede
old ones) on *active* chunk versions, labeled under the *current* decision
policy.

```text
RequirementSatisfied(r) := distinct-content active witness count for r > 0

GroupComplete(g) := every active requirement of g satisfied, with a distinct
                    content hash assignable to each requirement (D-1)

ClaimSupported(c) := direct distinct-content support count for c > 0
                     OR complete group count for c > 0

ClaimRefuted(c) := distinct-content refuting witness count for c > 0
```

Claim status:

```text
supported and refuted      -> CONFLICTED
supported and not refuted  -> SUPPORTED
not supported and refuted  -> REFUTED
otherwise                  -> UNSUPPORTED
```

Answer policy is derived from required claim states and should remain
configurable. Counts and provenance are canonical; display labels are policy.

## Update Transactions

### Delete

1. Mark the document and passage versions inactive.
2. Withdraw candidate, verification, and witness observations that reference
   those versions.
3. Find claims requiring replacement candidates.
4. Incrementally propagate relational deltas.
5. Retrieve and verify replacements only for affected claims.
6. Commit status deltas with the update-event identifier.

### Insert

1. Create immutable document and passage versions.
2. Embed new passages.
3. Run reverse candidate discovery against registered claims.
4. Verify only selected claim-passage pairs.
5. Insert semantic observations.
6. Incrementally propagate resulting deltas.

### Replace

Execute deletion of the old version and insertion of the new version in one
logical update event. Preserve both versions for auditability.

## Consistency Boundary

The database transaction should atomically publish:

- version activation/deactivation;
- semantic observation changes selected for the event;
- maintained claim and answer states; and
- status-delta provenance.

Long-running model calls should be staged outside the publication transaction.
The system must expose whether an update is pending semantic evaluation rather
than presenting an old state as confirmed-current.

## Initial Deployment Shape

```text
React/Next.js dashboard
        |
FastAPI service
        |
PostgreSQL + pgvector
        |
local embedding and verifier models
```

The first implementation may keep the reference engine in Python. A custom C++
or factorized kernel is considered only after correctness and workload traces
exist.
