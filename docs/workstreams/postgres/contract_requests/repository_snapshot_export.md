# Contract request: stable repository snapshot export

Status: nonblocking request for coordinator review.

## Problem

`PostgresSnapshot.capture` must serialize immutable history, validity
intervals, policy history, current observation currency, processed event
digests, and status deltas. `InMemoryRepository` exposes point/read APIs but no
complete typed export for those relations. The lane therefore reads its
private collections without mutating them.

## Requested shared change

Add a coordinator-owned immutable repository snapshot/export type or read-only
iterators covering:

- questions, answers, and claims;
- document/chunk versions with validity intervals;
- observations and current currency;
- policies with validity intervals;
- processed event identifiers/digests with revision mapping;
- status deltas.

The adapter can then depend on a public contract instead of `_...` fields.

## Current compatibility

This request does not block integration. The current adapter validates that
the processed-event count maps one-to-one to `current_epoch` and rejects an
incremental engine whose claim, answer, or certificate key sets are not
synchronized with the captured repository.
