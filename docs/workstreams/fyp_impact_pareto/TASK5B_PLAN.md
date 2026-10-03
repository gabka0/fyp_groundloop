# Task 5B Plan: Error Analysis and Offline Pareto Freeze

## Objective

Close the offline gaps between the frozen Task 4 `NO_GO` result and the Task
5A held-out selector checkpoint before any paid inference:

1. account for every Task 4 oracle-positive miss at pair and effect level;
2. expose why the vector, lexical, vector-first union, lineage, and frontier
   channels did or did not recover that pair;
3. evaluate the three Task 5A text selectors over the complete held-out budget
   frontier; and
4. freeze the exact hosted-verifier request population, cost ceiling, metric
   definitions, and execution stages for Task 5C.

This task does not modify M4/M5 contracts and does not call a model.

## Frozen evidence

- Task 4 controlled config:
  `configs/m4/evaluation/controlled_v1.json`.
- Task 4 fixture and runner are read-only.
- Task 5A config:
  `configs/fyp/impact_selection_v1.json`.
- Prepared VitaminC source SHA-256:
  `1a306620c363d52c6abfcdf5f6d272e7767cfdc8cbd364768b27901870a3c882`.
- Task 5A report manifest:
  `dac99fd0ffff4749b6defe6ad3975e593f4615d6a291050ce0920da875bea9fa`.

## Task 4 miss taxonomy

For every policy/event/pair in `all_missed_positive_pairs`, record:

- policy, history, event, claim and chunk identity;
- configured approximate and frontier budgets;
- admitted pairs and missed pair;
- vector, lexical, lineage, and frontier ranks or absence;
- whether each channel could recover the pair inside its own budget;
- approximate, lineage-extra, frontier-extra, attempted-pair and batch counts;
- exact pair, claim, status and answer-effect numerator/denominator; and
- one deterministic failure code plus an evidence-based explanation.

Allowed failure codes are:

- `channel_rank_outside_budget`;
- `vector_first_union_cap_exhaustion`;
- `lineage_points_to_other_claim`;
- `frontier_required_for_recovery`; and
- `unsupported_fixture_shape` (fail the report rather than silently use it).

The report must also record that frontier recovers the relevant first-version
pairs only by reaching the two-pair exhaustive event work in this fixture.

## Retrospective held-out Pareto

Task 5A selected one budget before inspecting the held-out partition. Task 5B
may now report the complete held-out frontier, but it must label that frontier
`retrospective_after_task5a_selection`; it is not a new preregistered test.

Evaluate:

- policies `changed_token_overlap`, `new_evidence_overlap`, and
  `old_new_rarity_coverage`;
- budgets `1, 2, 4, 8, 16, 32, 64, 256`; and
- exactly the frozen Task 5A evaluation partition.

For every point preserve integer counts for events, registry claims,
exhaustive pairs, selected pairs, avoided pairs, affected claims, and fully
covered events. Mark Pareto-optimal points only when no other point has both
at least its affected-claim recall and at most its selected-pair work, with
one strict improvement.

## Hosted-verifier freeze

Task 5B prepares but does not execute a provider request population:

- primary model: `gpt-6-luna`;
- API: Responses API;
- reasoning effort: `none`;
- output: strict one-field enum `support|refute|neutral`;
- no rationale in the measured path;
- evaluation partition only;
- one old-evidence and one new-evidence judgment for every event/registry
  claim pair;
- stable request identity over model, prompt/schema hashes, claim hash,
  old-evidence hash, new-evidence hash, and judged-side marker;
- exact byte-stable JSONL with no API key;
- estimated standard and Batch cost; and
- maximum authorized Task 5C spend of USD 4.00.

Gold labels, page identity, stratum, and expected affected identities may not
enter request construction. Gold is joined only after responses are frozen.

## Task 5C stages

1. Offline fake-provider and parser gates.
2. Development smoke with at most 32 billable requests and USD 0.05 cap.
3. Freeze prompt/schema bytes after development only.
4. Held-out selected path.
5. Held-out old plus exhaustive-new population through Batch, split beneath
   provider request/token limits.
6. Fixed-concurrency synchronous timing subset.
7. Compute label accuracy on the annotated own-page pairs and system-effect
   recall relative to the exhaustive hosted-model observations.

No later stage may start if the prior stage has incomplete responses, schema
failures, identity mismatches, or a projected cumulative cost above USD 4.00.

## Path-exclusive ownership

Task 5B/5C may change only:

- `src/groundloop/fyp_impact_pareto.py`;
- `src/groundloop/hosted_verifier.py`;
- Task 5B/5C command wiring in `src/groundloop/cli.py`;
- `configs/fyp/impact_pareto_v1.json`;
- `configs/fyp/hosted_verifier_openai_luna_v1.json`;
- `tests/m4/fyp_impact_pareto/**`;
- `tests/ai/hosted_verifier/**`;
- `docs/fyp_impact_pareto.md`;
- `docs/workstreams/fyp_impact_pareto/**`; and
- the Task 5B/5C README section.

The Task 4 runner/fixture, Task 5A module/config, all M4/M5 contracts, prepared
source artifacts, and model-result artifacts are read-only dependencies.

## Success boundary

Offline completion means every miss is accounted for and all request/Pareto
bytes are reproducible. It is not a speedup or AI-quality result. Final Task 5
success requires Task 5C to demonstrate simultaneous verifier-work reduction,
acceptable pair/claim/status/answer-effect retention, valid exhaustive oracle
agreement, and lower measured model/update work within the frozen cost cap.
