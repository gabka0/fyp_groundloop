# Task 5A Plan: Held-out Impact Selection Diagnostic

## Objective

Test whether a text-only impact selector can reduce the number of
claim--revision pairs sent to a verifier while retaining the claims whose
gold label changes across the revision.

This task isolates selection quality from verifier quality. It does not call
an LLM, measure verifier accuracy, or change any frozen M4 or M5 contract.
It is a checkpoint within the broader Task 5 error-analysis, Pareto, timing,
cost, and end-to-end effect program; it does not complete that program.

## Evidence boundary

The input is the exact public VitaminC development artifact prepared for
M4.13. Its expected SHA-256 is
`1a306620c363d52c6abfcdf5f6d272e7767cfdc8cbd364768b27901870a3c882`.
The artifact has already been used by the project, so this is an internal
bounded diagnostic rather than an untouched external evaluation.

Each case contains two claims evaluated against old and new evidence. Both
claims change gold label across the revision. For an evaluation event, those
two claims are the affected set and all evaluation claims form the candidate
registry. Gold labels may be used only after ranking.

## Frozen split and selection rule

1. Group rows by case and validate the exact four-row case structure.
2. Within each of the `support_refute` and `support_neutral` strata, sort cases
   by `(selection_key, case_id)`.
3. Use the first 64 cases per stratum as the development partition and the
   remaining 64 per stratum as the held-out evaluation partition.
4. Evaluate these text-only policy families on development:
   - `new_evidence_overlap`: multiset token overlap between the claim and the
     new evidence;
   - `changed_token_overlap`: overlap with inserted/replaced evidence tokens,
     using new-evidence overlap as a deterministic tie-break;
   - `old_new_rarity_coverage`: exact weighted claim-token coverage against
     both old and new evidence. A token's integer weight is the registry size
     divided by its claim-document frequency, rounded down to at least one.
     Rank by the greater old/new coverage, then the lesser coverage, using
     exact rational arithmetic.
5. Evaluate budgets `1, 2, 4, 8, 16, 32` candidates per event.
6. A development candidate is eligible only if it achieves all of:
   - at least `0.95` affected-claim recall;
   - at least `0.90` full-event coverage;
   - at least `0.80` pair-work reduction relative to exhaustive pairing.
7. Select the eligible candidate with the smallest budget, then the greatest
   affected-claim recall, greatest full-event coverage, greatest reduction,
   and lexicographically smallest policy identifier.
8. Run exactly that selected candidate once on the held-out partition. The
   held-out verdict is `PASS` only if it independently meets the same three
   thresholds; otherwise it is `FAIL`. If development has no eligible
   candidate, return `NO_CANDIDATE` without inspecting held-out labels.

The selector receives only claim text plus old and new evidence text. It may
not receive page, case, stratum, source label, gold label, or the expected
affected claim identities.

The third family was added after the initial two development candidates
returned `NO_CANDIDATE`; the held-out partition had not been evaluated. This
is development-set iteration, not an untouched preregistration.

## Path-exclusive ownership

Task 5 may change only:

- `src/groundloop/fyp_impact_selection.py`;
- the Task 5 command wiring in `src/groundloop/cli.py`;
- `configs/fyp/impact_selection_v1.json`;
- `tests/m4/fyp_impact_selection/**`;
- `docs/fyp_impact_selection.md`;
- `docs/workstreams/fyp_impact_selection/**`;
- the Task 5 section of `README.md`.

M3/M4/M5 contracts, model checkpoints, prepared source artifacts, and prior
results are read-only dependencies.

## Outputs

`groundloop fyp-impact-selection` writes:

- canonical JSON containing source/config hashes, validated population
  counts, every development result, the selected candidate, the held-out
  result, thresholds, verdict, and limitations;
- a development-frontier CSV;
- an evaluation-event CSV with integer affected/selected counts only; and
- a concise terminal summary.

Repeated runs over identical bytes must produce identical output bytes.

## Claim boundary

A passing result would show that this simple selector reduced candidate-pair
work while retaining affected claims on this bounded internal diagnostic. It
would not establish end-to-end speedup, monetary savings, verifier accuracy,
answer correctness, natural-history utility, or superiority to another
system. A later hosted-verifier experiment must measure those separately.
