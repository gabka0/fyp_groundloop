# GroundLoop Immediate Task 1 Execution Prompt

Act as the GroundLoop FYP primary-demo implementation agent.

Work in a new isolated worktree from current `origin/main`. Read `AGENTS.md`
and its required authority documents before making claims or edits. Preserve
the user-owned checkout and every dirty or held M5 worktree exactly; do not
reset, clean, stash, copy from, stage, or modify them.

## Goal

Turn the accepted M4.8 real dynamic history into one clear, runnable FYP demo
command. Reuse the existing production PostgreSQL coordinator and pinned local
M3 embedding/verifier adapters. Do not build a toy replacement engine and do
not alter the accepted M4.8 execution semantics.

The demo starts from M4.8's fixed pre-registered answer, claim, and initial
observation. It must execute the existing real-model `INSERT`, exact-withdrawal
`DELETE`, and real-model `REPLACE` history, then present:

- the claim and answer status after every event;
- discovery, embedding, and verifier work per event;
- persisted provenance counts;
- equality with the independent Python and SQL full-recomputation oracles;
- fresh-connection exact replay with zero discovery, embedding,
  verifier-request, and verifier-backend calls; and
- proof that replay leaves the durable database projection unchanged.

## Mandatory boundary

M3 static answer generation and M4 dynamic maintenance are currently separate
audited scenes. M4.8 seeds a fixed registered claim/answer and does not consume
an arbitrary `groundloop m3-register` result. Document this honestly. Do not
claim or silently implement a seamless M3-to-M4 bridge in this task.

The exact statement is only that, for the fixed registered state and the same
stored versioned neural observations, each successfully sealed event matches
independent structured recomputation. Keep objective truth, semantic
completeness, arbitrary-corpus usability, answer regeneration,
representative speedup, population-level recall/call savings, production
deployment, named-system superiority, M5 completion, and AI-quality
improvement explicitly unsupported. The frozen M3 verifier remains the
default after M4.13 `NO_GO`.

## Implementation

1. Write a path-exclusive implementation plan before editing source.
2. Add a small pure module that validates the canonical M4.8 manifest and
   formats a stable human-readable summary. Fail closed on a wrong schema,
   wrong event order, unsealed event, oracle mismatch, any replay discovery,
   embedding, verifier-request, or verifier-backend call, or replay database
   mutation.
3. Add `groundloop fyp-demo` as a thin CLI wrapper around
   `run_m4_real_dynamic_history`. It must accept the existing database,
   repository, artifact, model-config, lexical-config, output, and
   keep-schema inputs; write the unchanged canonical JSON manifest; and print
   the validated summary.
4. Add deterministic tests for summary content, CLI delegation, and all
   fail-closed conditions without loading models or requiring PostgreSQL.
5. Add a concise demo guide with prerequisites, exact command, expected
   trajectory, output interpretation, separate optional M3 static scene, and
   the evidence/non-claim boundary. Link it from the README.
6. Do not edit M4 runtime semantics, migrations, models, prompts, thresholds,
   M5 files, provider configuration, or runtime mode.

## Required verification

Run focused pytest, Ruff check, Ruff format check, strict mypy, compileall, and
`git diff --check`. If the pinned local artifacts and PostgreSQL service are
available, execute `groundloop fyp-demo` once with downloads disabled and
verify the generated manifest. Never commit generated output, model artifacts,
database data, secrets, or caches.

Finish with the exact changed paths, exact command results, the observed demo
trajectory, and remaining unsupported claims. Do not call the whole FYP or M5
complete merely because this packaging task passes.
