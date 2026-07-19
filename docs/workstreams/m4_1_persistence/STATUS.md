# M4.1 PostgreSQL Runtime Lane Status

Status: implementation complete on the lane branch; awaiting coordinator
review and merge.

## Implemented

- Canonical, content-derived candidate-policy registration and exact replay.
- Serialized structural epoch opening with immutable update, roots and global
  discovery-scope membership.
- D-19 composition with a coordinator-supplied structural mutation callback
  inside the epoch-opening transaction.
- Canonical SQL-to-`RuntimeEpoch` and SQL-to-`RuntimeBook` projection.
- Attempt leasing, retryable failure and contiguous attempt history.
- Atomic expandable completion with exact child declaration, dependency
  edges, child-set closure and discovery-scope close.
- Non-expandable verifier completion.
- Revision CAS, exact completion/event/attempt/failure replay and conflicting
  content rejection.
- Failed-epoch preservation and late inactive completion.
- Sealing around an injected coordinator publication action; the transaction
  verifies that the action advanced `groundloop_m4_publication_head`.
- Publication-head authority for the next event's B0 identity, with a
  `max(sealed epoch)` fallback only before migration 004's head is initialized.
- Pure-model comparison of the uncommitted SQL projection before every
  transition transaction commits.

The store does not modify `groundloop_observation_currency`, working
observation deltas, published observation currency, claim/answer state or
public deltas.

## Validation

From the lane worktree with the live database configured:

```bash
GROUNDLOOP_TEST_DATABASE_URL="$GROUNDLOOP_DATABASE_URL" \
  .venv/bin/pytest -q \
  tests/m4/persistence tests/m4/runtime tests/m4/test_m4_contracts.py
.venv/bin/ruff check src/groundloop/m4/persistence.py tests/m4/persistence
.venv/bin/mypy --strict src/groundloop/m4/persistence.py
.venv/bin/python -m compileall -q \
  src/groundloop/m4/persistence.py tests/m4/persistence
git diff --check main...HEAD
```

Observed before this documentation-only commit:

- 103 focused/shared tests passed against live PostgreSQL.
- Eight persistence tests passed in isolated schemas.
- Ruff passed.
- Strict mypy passed for the owned source module.
- Compileall passed.
- Ownership and whitespace checks passed.

## Nonclaims

This is coordination persistence, not end-to-end M4. It does not prove
grounding-state equality, neural quality, admission recall, publication
correctness or latency improvement. The coordinator must supply the actual
publication callback and compare Surfaces A-C in integration.
