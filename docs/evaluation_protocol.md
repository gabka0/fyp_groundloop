# GroundLoop Evaluation Protocol

## Objective

Evaluate database correctness, AI quality, and systems efficiency separately
before reporting end-to-end performance.

## Two Oracles

### Relational correctness oracle

Input: an identical snapshot of stored, versioned semantic observations.

Compare:

```text
incremental maintenance result
    ==
full relational recomputation result
```

Run the comparison after every update, including intermediate zero crossings
and alternative-witness cases. This path should be deterministic.

### End-to-end semantic oracle

Input: corpus version, registered claims, fixed model versions, prompts,
thresholds, and decoding configuration.

Compare the selective pipeline with a full retrieval-and-verification rerun.
Because semantic components may be approximate or stochastic, report agreement,
recall, confidence intervals, and repeated-run variance where applicable.

## Update Workloads

Include:

- Single supporting passage deletion.
- Final supporting passage deletion.
- Alternative support insertion.
- Contradictory evidence insertion.
- Supporting passage replacement with neutral evidence.
- Supporting passage replacement with refuting evidence.
- Deletion of one requirement in a conjunctive evidence group.
- Insertion of an alternative complete evidence group.
- Local updates affecting one answer.
- Shared-source updates affecting many answers.
- Mixed insert/delete/replace streams.

Record update locality, fanout, corpus size, registered answers, registered
claims, evidence edges, and evidence groups for every workload.

## Baselines

1. Full retrieval and verification recomputation.
2. Full answer regeneration where affordable.
3. Source-level invalidation.
4. Direct-citation invalidation.
5. TTL or freshness-risk refresh.
6. GroundLoop with direct witnesses only.
7. GroundLoop with bounded evidence groups.

All baselines must receive the same active corpus versions and model versions.

## Systems Metrics

- Update latency: median, p95, and p99.
- Updates per second.
- Claims and answers touched per update.
- Retrieval calls and candidates scored.
- Verifier calls.
- Generated and processed tokens where applicable.
- Maintained-state size.
- Full-recomputation speedup.
- Maintenance overhead when no status changes.
- Break-even registry size and update locality.

## AI Metrics

- Evidence retrieval recall at k.
- Support/refute/neutral precision, recall, macro-F1, and confusion matrix.
- Affected-claim recall and precision.
- Unsupported-answer and contradiction detection.
- False invalidation rate.
- Stale-answer exposure time.
- Expected calibration error and Brier score.
- Regeneration precision.

Affected-claim recall is the primary impact-selection safety metric. A false
negative leaves a stale answer active; a false positive mainly wastes compute.

## Required Ablations

- No reverse provenance.
- Embedding-only impact selection.
- Lexical/entity-only impact selection.
- Combined impact selection.
- Direct witnesses versus evidence groups.
- Zero-shot versus adapted verifier.
- Different candidate budgets `L` and retrieval depths `k`.
- With and without confidence calibration.

## Dataset Strategy

Use at least:

1. one public claim-verification dataset adapted into controlled update
   streams; and
2. one naturally versioned document collection for the end-to-end demo.

Candidate public resources include FEVER, SciFact, WiCE, AVeriTeC, and HoH.
Software or API documentation histories are preferred for the naturally
versioned corpus.

Generated update streams must record their transformation procedure and random
seed. Manually curated subsets must preserve an annotation guide and adjudicated
labels.

## Statistical Reporting

- Use fixed dataset splits and publish seeds.
- Report the number of update events and independent corpora.
- Include confidence intervals for sampled semantic metrics.
- Use paired comparisons because policies process the same update events.
- Report failures, timeouts, and unaffordable full-recomputation cases.
- Do not report only averages when fanout or latency has a heavy tail.

## Minimum Acceptance Tests

- Incremental and full relational results agree after every generated event.
- Deleting a non-final witness does not invalidate a claim.
- Deleting a final witness does invalidate it unless another group survives.
- Support plus refutation produces conflict rather than silent precedence.
- Replaying the same event identifier is idempotent.
- Old passage versions remain auditable but inactive.
- Every externally visible status transition has an associated `StatusDelta`.
