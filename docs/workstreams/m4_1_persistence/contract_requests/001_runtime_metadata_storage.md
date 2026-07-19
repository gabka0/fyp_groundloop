# Contract Request: Dedicated Runtime Metadata Storage

Status: nonblocking request for coordinator decision.

## Gap

Migration 003 cannot reconstruct two fields required by the frozen pure
runtime projection:

1. `RuntimeEpoch.failure_reason` has no relational column.
2. An `all_registered_claims` discovery scope stores a registry snapshot ID,
   but there is no immutable registry-membership relation from which its exact
   historical `registered_claim_ids` can be reconstructed.

Without these values, exact failure replay/conflict detection and historical
PENDING reconstruction are impossible.

## Lane-local compatibility representation

The adapter stores the values under
`groundloop_m4_update.manifest._groundloop_m4_runtime_v1`, together with the
original event manifest. This is fully transactional and tested, but failure
updates make the JSON column partially mutable even though the update identity
is conceptually immutable.

## Recommended follow-up

Add a dedicated runtime-failure table keyed by epoch and an immutable
registry-snapshot membership relation keyed by `(registry_snapshot_id,
claim_id)`. Then migrate the adapter away from mutable update-manifest
metadata. Do not block M4.1 deterministic integration on this normalization;
the current representation is exact and inspectable.
