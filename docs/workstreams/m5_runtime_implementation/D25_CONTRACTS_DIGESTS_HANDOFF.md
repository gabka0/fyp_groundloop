# M5-D25 Wave 1 Lane A Contracts/Digests Handoff

Status: scoped pure contracts/digests candidate complete; D25 implementation
remains `PENDING`

Date: 2026-09-03

## Authority and scope

This lane started from activation commit
`691e3d174e059ac041d3a46678fcb630e15478d5` and the accepted D25 amendment at
SHA-256
`bac12ab5e74632c04f1bd70d0ef0d00522ba9d268eb8b73d11845bbf3b873aae`.
It changed only the five Lane A paths authorized by
`D25_MIGRATION_017_ACTIVATION.md`.

## Implemented pure surface

- exact enums and immutable DTOs for matching layers, sources, logical and
  binding kinds, audit families/errors, and provenance kinds;
- lossless current/working observation, edge, mask and bounded-Hall points,
  including explicit working tombstones and revision-zero current points;
- complete bounded-Hall validation, including `r in 1..8`, negative
  deficiencies, complement/intersection neighbour recomputation, and exact
  maximum-deficiency, matching-size and distinct-count formulas;
- exact physical changes, image point, group shape, transition intent,
  persisted patch, contribution, receipt, logical change and certificate
  binding contracts;
- complete immutable aggregate physical/logical patch artifacts retaining and
  recomputing child, logical-output, logical-overlay, 37-counter work and outer
  patch identities from their exact canonical preimages, including source-kind
  point admissibility, explicit after images, binding-epoch equality and
  derivable D22 after-hash checks (unchanged group/claim certificate inputs are
  intentionally deferred to locked store composition because migration 017
  does not retain them);
- explicitly named 37-field persisted work vector and an exact flattening of
  the accepted `M5OverlayWork` receipt/contribution type;
- exact D25 schema, group-shape, point/change, logical patch, binding, patch,
  contribution, intent, audit projection/row/mismatch, provenance and audit
  digest helpers;
- the accepted nine-block `m5-overlay-logical-output-v2` encoder using an
  exact concrete-type and field allowlist, preserving the 71-byte empty image;
  and
- canonical key/order, exact-type, option/error, mutation, tombstone/absence,
  revision, skipped-provenance and PASS-shape falsifiers.

No PostgreSQL module, migration, store, runtime composition, provider,
database, deployment, or runtime mode was touched.

## Executed gates

- focused owned tests: **134 passed**;
- complete pure M5 runtime directory: **414 passed**;
- M4 public API plus legacy compatibility: **14 passed**;
- Ruff over both owned source and test files: passed;
- strict mypy over both owned source files: passed;
- compileall over the M5 runtime source/tests: passed;
- `git diff --check`: passed;
- no skips, xfails, deselection, partial pooling, database, or network use.

## Remaining boundary

This lane supplies only Wave 1's pure byte-contract surface. Migration 017,
schema/installer evidence, store implementation, transition composition,
physical audit orchestration, reconnect/seal histories, and the rest of the
40-falsifier matrix remain later barriers. M5-D25 and M5.0-25 therefore remain
contract-`PASS` / implementation-`PENDING`; runtime remains `v1_only` outside
isolated fixtures. An independent same-byte audit is required before
integration.
